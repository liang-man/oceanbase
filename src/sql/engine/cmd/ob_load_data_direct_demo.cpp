#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_load_data_direct_demo.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"

#include <iostream>    
#include <fstream>
#include <string>
#include <thread>
#include <vector> 
#include <numeric>
#include <chrono>
#include <semaphore.h> 
#include <pthread.h>
#include <mutex>
#include <sstream>
#include <atomic>
#include <deque>
#include "share/ob_thread_pool.h"
// #include <sys/sysinfo.h>
// #include <unistd.h>

std::mutex mtx;

namespace oceanbase
{
namespace sql
{
using namespace blocksstable;
using namespace common;
using namespace lib;
using namespace observer;
using namespace share;
using namespace share::schema;

/**
 * ObLoadDataBuffer
 */

ObLoadDataBuffer::ObLoadDataBuffer()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), data_(nullptr), begin_pos_(0), end_pos_(0), capacity_(0)
{
}

ObLoadDataBuffer::~ObLoadDataBuffer()
{
  reset();
}

void ObLoadDataBuffer::reuse()
{
  begin_pos_ = 0;
  end_pos_ = 0;
}

void ObLoadDataBuffer::reset()
{
  allocator_.reset();
  data_ = nullptr;
  begin_pos_ = 0;
  end_pos_ = 0;
  capacity_ = 0;
}

int ObLoadDataBuffer::create(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != data_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataBuffer init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(data_ = static_cast<char *>(allocator_.alloc(capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(capacity));
    } else {
      capacity_ = capacity;
    }
  }
  return ret;
}

int ObLoadDataBuffer::squash()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataBuffer not init", KR(ret), KP(this));
  } else {
    // sem_wait(semLock);   // 凡是修改、调用公共量，都得加锁
    const int64_t data_size = get_data_size();   // data_size一开始为0  执行end_pos_ - begin_pos_
    if (data_size > 0) {
      MEMMOVE(data_, data_ + begin_pos_, data_size);
    }
    begin_pos_ = 0;
    end_pos_ = data_size;
  }
  return ret;
}

/**
 * ObLoadSequentialFileReader
 */

ObLoadSequentialFileReader::ObLoadSequentialFileReader()
  : offset_(0), is_read_end_(false)
{
}

ObLoadSequentialFileReader::~ObLoadSequentialFileReader()
{
}

int ObLoadSequentialFileReader::open(const ObString &filepath)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_reader_.open(filepath, false))) {
    LOG_WARN("fail to open file", KR(ret));
  }
  return ret;
}

int ObLoadSequentialFileReader::read_next_buffer(ObLoadDataBuffer &buffer)
{
  // sem_wait(semLock);
  // mtx.lock();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_reader_.is_opened())) {
    ret = OB_FILE_NOT_OPENED;
    LOG_WARN("file not opened", KR(ret));
  } else if (is_read_end_) {
    ret = OB_ITER_END;
  } else if (OB_LIKELY(buffer.get_remain_size() > 0)) {
    const int64_t buffer_remain_size = buffer.get_remain_size();   // buffer_remain_size一开始为2097152，即2M 
    int64_t read_size = 0;
    // mtx.lock();
    if (OB_FAIL(file_reader_.pread(buffer.end(), buffer_remain_size, offset_, read_size))) {   // 读取2M的数据
      LOG_WARN("fail to do pread", KR(ret));
    } else if (read_size == 0) {
      is_read_end_ = true;
      ret = OB_ITER_END;
    } else {
      // 这个offset_就是下一轮2M数据的起点
      // offset_是一个公共量，多线程修改时要加锁
      // sem_wait(semLock);           // 凡是修改、调用公共量，都得加锁
      offset_ += read_size;        // 只要csv文件超过2M，那么这个read_size基本上都是2097152
      // sem_post(semLock);
      buffer.produce(read_size);   // 执行end_pos_ += read_size

      // 从当前end_pos_的位置往前找最近的'\n'的位置
      // 我们希望的是：'\n'就在end_pos_的位置, 这样就表明是完整的行，不会多出来几个字节
      // char *ptr = nullptr;
      // ptr = strrchr(buffer.data(), '\n');
      // int surplus = buffer.end() - ptr - 1;
      // buffer.set_begin(buffer.end_pos() - surplus);
    }
    // mtx.unlock();
  }
  // sem_post(semLock);
  // mtx.unlock();
  return ret;
}

/**
 * ObLoadCSVPaser
 */

ObLoadCSVPaser::ObLoadCSVPaser()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), collation_type_(CS_TYPE_INVALID), is_inited_(false)
{
}

ObLoadCSVPaser::~ObLoadCSVPaser()
{
  reset();
}

void ObLoadCSVPaser::reset()
{
  allocator_.reset();
  collation_type_ = CS_TYPE_INVALID;
  row_.reset();
  err_records_.reset();
  is_inited_ = false;
}

int ObLoadCSVPaser::init(const ObDataInFileStruct &format, int64_t column_count,
                         ObCollationType collation_type)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadCSVPaser init twice", KR(ret), KP(this));
  } else if (OB_FAIL(csv_parser_.init(format, column_count, collation_type))) {
    LOG_WARN("fail to init csv parser", KR(ret));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    ObObj *objs = nullptr;
    if (OB_ISNULL(objs = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * column_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      new (objs) ObObj[column_count];
      row_.cells_ = objs;
      row_.count_ = column_count;
      collation_type_ = collation_type;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadCSVPaser::get_next_row(ObLoadDataBuffer &buffer, const ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (buffer.empty()) {
    ret = OB_ITER_END;
  } else {
    const char *str = buffer.begin();
    const char *end = buffer.end();
    int64_t nrows = 1;
    if (OB_FAIL(csv_parser_.scan(str, end, nrows, nullptr, nullptr, unused_row_handler_,
                                 err_records_, false))) {
      LOG_WARN("fail to scan buffer", KR(ret));
    } else if (OB_UNLIKELY(!err_records_.empty())) {
      ret = err_records_.at(0).err_code;
      LOG_WARN("fail to parse line", KR(ret));
    } else if (0 == nrows) {
      ret = OB_ITER_END;
    } else {
      buffer.consume(str - buffer.begin());    // str - buffer.begin()长度为119，为一行记录的字节长度
      const ObIArray<ObCSVGeneralParser::FieldValue> &field_values_in_file =
        csv_parser_.get_fields_per_line();
      // 对这一行的每个字段做类型转换，都转为string
      for (int64_t i = 0; i < row_.count_; ++i) {
        const ObCSVGeneralParser::FieldValue &str_v = field_values_in_file.at(i);
        ObObj &obj = row_.cells_[i];   // cells是所有字段的集合
        if (str_v.is_null_) {
          obj.set_null();
        } else {
          obj.set_string(ObVarcharType, ObString(str_v.len_, str_v.ptr_));
          obj.set_collation_type(collation_type_);
        }
      }
      row = &row_;
    }
  }
  return ret;
}

/**
 * ObLoadDatumRow
 */

ObLoadDatumRow::ObLoadDatumRow()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), capacity_(0), count_(0), datums_(nullptr)
{
}

ObLoadDatumRow::~ObLoadDatumRow()
{
}

void ObLoadDatumRow::reset()
{
  allocator_.reset();
  capacity_ = 0;
  count_ = 0;
  datums_ = nullptr;
}

int ObLoadDatumRow::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else {
    reset();
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(datums_ = static_cast<ObStorageDatum *>(
                    allocator_.alloc(sizeof(ObStorageDatum) * capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      new (datums_) ObStorageDatum[capacity];
      capacity_ = capacity;
      count_ = capacity;
    }
  }
  return ret;
}

int64_t ObLoadDatumRow::get_deep_copy_size() const
{
  int64_t size = 0;
  size += sizeof(ObStorageDatum) * count_;
  for (int64_t i = 0; i < count_; ++i) {
    size += datums_[i].get_deep_copy_size();
  }
  return size;
}

int ObLoadDatumRow::deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src));
  } else {
    reset();
    ObStorageDatum *datums = nullptr;
    const int64_t datum_cnt = src.count_;
    datums = new (buf + pos) ObStorageDatum[datum_cnt];
    pos += sizeof(ObStorageDatum) * datum_cnt;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt; ++i) {
      if (OB_FAIL(datums[i].deep_copy(src.datums_[i], buf, len, pos))) {
        LOG_WARN("fail to deep copy storage datum", KR(ret), K(src.datums_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      capacity_ = datum_cnt;
      count_ = datum_cnt;
      datums_ = datums;
    }
  }
  return ret;
}

DEF_TO_STRING(ObLoadDatumRow)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(capacity), K_(count));
  if (nullptr != datums_) {
    J_ARRAY_START();
    for (int64_t i = 0; i < count_; ++i) {
      databuff_printf(buf, buf_len, pos, "col_id=%ld:", i);
      pos += datums_[i].storage_to_string(buf + pos, buf_len - pos);
      databuff_printf(buf, buf_len, pos, ",");
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObLoadDatumRow)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(datums_, count_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLoadDatumRow)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", K(count));
    } else if (count > capacity_ && OB_FAIL(init(count))) {
      LOG_WARN("fail to init", KR(ret));
    } else {
      OB_UNIS_DECODE_ARRAY(datums_, count);
      count_ = count;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLoadDatumRow)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(datums_, count_);
  return len;
}

/**
 * ObLoadDatumRowCompare
 */

ObLoadDatumRowCompare::ObLoadDatumRowCompare()
  : result_code_(OB_SUCCESS), rowkey_column_num_(0), datum_utils_(nullptr), is_inited_(false)
{
}

ObLoadDatumRowCompare::~ObLoadDatumRowCompare()
{
}

int ObLoadDatumRowCompare::init(int64_t rowkey_column_num, const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDatumRowCompare init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(rowkey_column_num <= 0 || nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(rowkey_column_num), KP(datum_utils));
  } else {
    rowkey_column_num_ = rowkey_column_num;
    datum_utils_ = datum_utils;
    is_inited_ = true;
  }
  return ret;
}

bool ObLoadDatumRowCompare::operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDatumRowCompare not init", KR(ret), KP(this));
  } else if (OB_ISNULL(lhs) || OB_ISNULL(rhs) ||
             OB_UNLIKELY(!lhs->is_valid() || !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(lhs), KPC(rhs));
  } else if (OB_UNLIKELY(lhs->count_ < rowkey_column_num_ || rhs->count_ < rowkey_column_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", KR(ret), KPC(lhs), KPC(rhs), K_(rowkey_column_num));
  } else {
    if (OB_FAIL(lhs_rowkey_.assign(lhs->datums_, rowkey_column_num_))) {
      LOG_WARN("fail to assign datum rowkey", KR(ret), K(lhs), K_(rowkey_column_num));
    } else if (OB_FAIL(rhs_rowkey_.assign(rhs->datums_, rowkey_column_num_))) {
      LOG_WARN("fail to assign datum rowkey", KR(ret), K(rhs), K_(rowkey_column_num));
    } else if (OB_FAIL(lhs_rowkey_.compare(rhs_rowkey_, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare rowkey", KR(ret), K(rhs_rowkey_), K(rhs_rowkey_), KP(datum_utils_));
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

/**
 * ObLoadRowCaster
 */

ObLoadRowCaster::ObLoadRowCaster()
  : column_count_(0),
    collation_type_(CS_TYPE_INVALID),
    cast_allocator_(ObModIds::OB_SQL_LOAD_DATA),
    is_inited_(false)
{
}

ObLoadRowCaster::~ObLoadRowCaster()
{
}

int ObLoadRowCaster::init(const ObTableSchema *table_schema,
                          const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadRowCaster init twice", KR(ret));
  } else if (OB_UNLIKELY(nullptr == table_schema || field_or_var_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(field_or_var_list));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(MTL_ID(), tz_info_.get_tz_map_wrap()))) {
    LOG_WARN("fail to get tenant time zone", KR(ret));
  } else if (OB_FAIL(init_column_schemas_and_idxs(table_schema, field_or_var_list))) {
    LOG_WARN("fail to init column schemas and idxs", KR(ret));
  } else if (OB_FAIL(datum_row_.init(table_schema->get_column_count()))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else {
    column_count_ = table_schema->get_column_count();
    collation_type_ = table_schema->get_collation_type();
    cast_allocator_.set_tenant_id(MTL_ID());
    is_inited_ = true;
  }
  return ret;
}

int ObLoadRowCaster::init_column_schemas_and_idxs(
  const ObTableSchema *table_schema,
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 64> column_descs;
  if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
    LOG_WARN("fail to get column descs", KR(ret), KPC(table_schema));
  } else {
    bool found_column = true;
    for (int64_t i = 0; OB_SUCC(ret) && OB_LIKELY(found_column) && i < column_descs.count(); ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema", KR(ret), K(col_desc));
      } else if (OB_UNLIKELY(col_schema->is_hidden())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected hidden column", KR(ret), K(i), KPC(col_schema));
      } else if (OB_FAIL(column_schemas_.push_back(col_schema))) {
        LOG_WARN("fail to push back column schema", KR(ret));
      } else {
        found_column = false;
      }
      // find column in source data columns
      for (int64_t j = 0; OB_SUCC(ret) && OB_LIKELY(!found_column) && j < field_or_var_list.count();
           ++j) {
        const ObLoadDataStmt::FieldOrVarStruct &field_or_var_struct = field_or_var_list.at(j);
        if (col_desc.col_id_ == field_or_var_struct.column_id_) {
          found_column = true;
          if (OB_FAIL(column_idxs_.push_back(j))) {
            LOG_WARN("fail to push back column idx", KR(ret), K(column_idxs_), K(i), K(col_desc),
                     K(j), K(field_or_var_struct));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!found_column)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported incomplete column data", KR(ret), K(column_idxs_), K(column_descs),
               K(field_or_var_list));
    }
  }
  return ret;
}

int ObLoadRowCaster::get_casted_row(const ObNewRow &new_row, const ObLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadRowCaster not init", KR(ret));
  } else {
    const int64_t extra_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    cast_allocator_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_idxs_.count(); ++i) {
      int64_t column_idx = column_idxs_.at(i);
      if (OB_UNLIKELY(column_idx < 0 || column_idx >= new_row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column idx", KR(ret), K(column_idx), K(new_row.count_));
      } else {
        const ObColumnSchemaV2 *column_schema = column_schemas_.at(i);
        const ObObj &src_obj = new_row.cells_[column_idx];
        ObStorageDatum &dest_datum = datum_row_.datums_[i];
        if (OB_FAIL(cast_obj_to_datum(column_schema, src_obj, dest_datum))) {
          LOG_WARN("fail to cast obj to datum", KR(ret), K(src_obj));
        }
      }
    }
    if (OB_SUCC(ret)) {
      datum_row = &datum_row_;
    }
  }
  return ret;
}

int ObLoadRowCaster::cast_obj_to_datum(const ObColumnSchemaV2 *column_schema, const ObObj &obj,
                                       ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDataTypeCastParams cast_params(&tz_info_);
  ObCastCtx cast_ctx(&cast_allocator_, &cast_params, CM_NONE, collation_type_);
  const ObObjType expect_type = column_schema->get_meta_type().get_type();
  ObObj casted_obj;
  if (obj.is_null()) {
    casted_obj.set_null();
  } else if (is_oracle_mode() && (obj.is_null_oracle() || 0 == obj.get_val_len())) {
    casted_obj.set_null();
  } else if (is_mysql_mode() && 0 == obj.get_val_len() && !ob_is_string_tc(expect_type)) {
    ObObj zero_obj;
    zero_obj.set_int(0);
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, zero_obj, casted_obj))) {
      LOG_WARN("fail to do to type", KR(ret), K(zero_obj), K(expect_type));
    }
  } else {
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, obj, casted_obj))) {
      LOG_WARN("fail to do to type", KR(ret), K(obj), K(expect_type));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum.from_obj_enhance(casted_obj))) {
      LOG_WARN("fail to from obj enhance", KR(ret), K(casted_obj));
    }
  }
  return ret;
}

/**
 * ObLoadExternalSort
 */

ObLoadExternalSort::ObLoadExternalSort()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), is_closed_(false), is_inited_(false)
{
}

ObLoadExternalSort::~ObLoadExternalSort()
{
  external_sort_.clean_up();
}

int ObLoadExternalSort::init(const ObTableSchema *table_schema, int64_t mem_size,
                             int64_t file_buf_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadExternalSort init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
    ObArray<ObColDesc> multi_version_column_descs;
    if (OB_FAIL(table_schema->get_multi_version_column_descs(multi_version_column_descs))) {
      LOG_WARN("fail to get multi version column descs", KR(ret));
    } else if (OB_FAIL(datum_utils_.init(multi_version_column_descs, rowkey_column_num,
                                         is_oracle_mode(), allocator_))) {
      LOG_WARN("fail to init datum utils", KR(ret));
    } else if (OB_FAIL(compare_.init(rowkey_column_num, &datum_utils_))) {
      LOG_WARN("fail to init compare", KR(ret));
    } else if (OB_FAIL(external_sort_.init(mem_size, file_buf_size, 0, MTL_ID(), &compare_))) {
      LOG_WARN("fail to init external sort", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

// SpinlLock splck;   // 自旋锁 如果锁内操作很费时，就不要用自旋锁，用互斥锁
// std::mutex mtx;       // 互斥锁
int ObLoadExternalSort::append_row(const ObLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  // splck.lock();
  // mtx.lock();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.add_item(datum_row))) {
    LOG_WARN("fail to add item", KR(ret));
  }
  // splck.unlock();
  // mtx.unlock();
  return ret;
}

int ObLoadExternalSort::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.do_sort(true))) {
    LOG_WARN("fail to do sort", KR(ret));
  } else {
    is_closed_ = true;
  }
  return ret;
}

int ObLoadExternalSort::get_next_row(const ObLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadExternalSort not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected not closed external sort", KR(ret));
  } else if (OB_FAIL(external_sort_.get_next_item(datum_row))) {
    LOG_WARN("fail to get next item", KR(ret));
  }
  return ret;
}

/**
 * ObLoadSSTableWriter
 */

ObLoadSSTableWriter::ObLoadSSTableWriter()
  : rowkey_column_num_(0),
    extra_rowkey_column_num_(0),
    column_count_(0),
    is_closed_(false),
    is_inited_(false)
{
}

ObLoadSSTableWriter::~ObLoadSSTableWriter()
{
}

int ObLoadSSTableWriter::init(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadSSTableWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    tablet_id_ = table_schema->get_tablet_id();
    rowkey_column_num_ = table_schema->get_rowkey_column_num();
    extra_rowkey_column_num_ = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_count_ = table_schema->get_column_count();
    ObLocationService *location_service = nullptr;
    bool is_cache_hit = false;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    if (OB_ISNULL(location_service = GCTX.location_service_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("location service is null", KR(ret), KP(location_service));
    } else if (OB_FAIL(
                 location_service->get(MTL_ID(), tablet_id_, INT64_MAX, is_cache_hit, ls_id_))) {
      LOG_WARN("fail to get ls id", KR(ret), K(tablet_id_));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle_, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls_id_));
    } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls should not be null", KR(ret));
    } else if (OB_FAIL(ls->get_tablet(tablet_id_, tablet_handle_))) {
      LOG_WARN("fail to get tablet handle", KR(ret), K(tablet_id_));
    } else if (OB_FAIL(init_sstable_index_builder(table_schema))) {
      LOG_WARN("fail to init sstable index builder", KR(ret));
    } else if (OB_FAIL(init_macro_block_writer(table_schema))) {
      LOG_WARN("fail to init macro block writer", KR(ret));
    } else if (OB_FAIL(datum_row_.init(column_count_ + extra_rowkey_column_num_))) {
      LOG_WARN("fail to init datum row", KR(ret));
    } else {
      table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
      table_key_.tablet_id_ = tablet_id_;
      table_key_.log_ts_range_.start_log_ts_ = 0;
      table_key_.log_ts_range_.end_log_ts_ = ObTimeUtil::current_time_ns();
      datum_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row_.mvcc_row_flag_.set_last_multi_version_row(true);
      // 往主键字段后面加两个多版本字段
      datum_row_.storage_datums_[rowkey_column_num_].set_int(-1); // fill trans_version
      datum_row_.storage_datums_[rowkey_column_num_ + 1].set_int(0); // fill sql_no
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadSSTableWriter::init_sstable_index_builder(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc data_desc;
  if (OB_FAIL(data_desc.init(*table_schema, ls_id_, tablet_id_, MAJOR_MERGE, 1L))) {
    LOG_WARN("fail to init data desc", KR(ret));
  } else {
    data_desc.row_column_count_ = data_desc.rowkey_column_count_ + 1;
    data_desc.need_prebuild_bloomfilter_ = false;
    data_desc.col_desc_array_.reset();
    if (OB_FAIL(data_desc.col_desc_array_.init(data_desc.row_column_count_))) {
      LOG_WARN("fail to reserve column desc array", KR(ret));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(data_desc.col_desc_array_))) {
      LOG_WARN("fail to get rowkey column ids", KR(ret));
    } else if (OB_FAIL(
                 ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(data_desc.col_desc_array_))) {
      LOG_WARN("fail to add extra rowkey cols", KR(ret));
    } else {
      ObObjMeta meta;
      meta.set_varchar();
      meta.set_collation_type(CS_TYPE_BINARY);
      ObColDesc col;
      col.col_id_ = static_cast<uint64_t>(data_desc.row_column_count_ + OB_APP_MIN_COLUMN_ID);
      col.col_type_ = meta;
      col.col_order_ = DESC;
      if (OB_FAIL(data_desc.col_desc_array_.push_back(col))) {
        LOG_WARN("fail to push back last col for index", KR(ret), K(col));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_index_builder_.init(data_desc))) {
      LOG_WARN("fail to init index builder", KR(ret), K(data_desc));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::init_macro_block_writer(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_store_desc_.init(*table_schema, ls_id_, tablet_id_, MAJOR_MERGE, 1))) {
    LOG_WARN("fail to init data_store_desc", KR(ret), K(tablet_id_));
  } else {
    data_store_desc_.sstable_index_builder_ = &sstable_index_builder_;
  }
  if (OB_SUCC(ret)) {
    ObMacroDataSeq data_seq;
    if (OB_FAIL(macro_block_writer_.open(data_store_desc_, data_seq))) {
      LOG_WARN("fail to init macro block writer", KR(ret), K(data_store_desc_), K(data_seq));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::append_row(const ObLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() || datum_row.count_ != column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), K(column_count_));
  } else {
    for (int64_t i = 0; i < column_count_; ++i) {
      if (i < rowkey_column_num_) {
        datum_row_.storage_datums_[i] = datum_row.datums_[i];
      } else {
        datum_row_.storage_datums_[i + extra_rowkey_column_num_] = datum_row.datums_[i];
      }
    }
    if (OB_FAIL(macro_block_writer_.append_row(datum_row_))) {
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  return ret;
}

int  ObLoadSSTableWriter::create_sstable()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  SMART_VAR(ObSSTableMergeRes, merge_res)    // merge_res是最终的一个结果，从sstable_index_build里取出来的
  {
    const ObStorageSchema &storage_schema = tablet_handle_.get_obj()->get_storage_schema();
    int64_t column_count = 0;
    if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(column_count))) {
      LOG_WARN("fail to get stored column count in sstable", KR(ret));
    } else if (OB_FAIL(sstable_index_builder_.close(column_count, merge_res))) {    // 根据这个结果构造一个参数column_count
      LOG_WARN("fail to close sstable index builder", KR(ret));
    } else {
      ObTabletCreateSSTableParam create_param;
      create_param.table_key_ = table_key_;
      create_param.table_mode_ = storage_schema.get_table_mode_struct();
      create_param.index_type_ = storage_schema.get_index_type();
      create_param.rowkey_column_cnt_ = storage_schema.get_rowkey_column_num() +
                                        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      create_param.schema_version_ = storage_schema.get_schema_version();
      create_param.create_snapshot_version_ = 0;
      ObSSTableMergeRes::fill_addr_and_data(merge_res.root_desc_, create_param.root_block_addr_,
                                            create_param.root_block_data_);
      ObSSTableMergeRes::fill_addr_and_data(merge_res.data_root_desc_,
                                            create_param.data_block_macro_meta_addr_,
                                            create_param.data_block_macro_meta_);
      create_param.root_row_store_type_ = merge_res.root_desc_.row_type_;
      create_param.data_index_tree_height_ = merge_res.root_desc_.height_;
      create_param.index_blocks_cnt_ = merge_res.index_blocks_cnt_;
      create_param.data_blocks_cnt_ = merge_res.data_blocks_cnt_;
      create_param.micro_block_cnt_ = merge_res.micro_block_cnt_;
      create_param.use_old_macro_block_count_ = merge_res.use_old_macro_block_count_;
      create_param.row_count_ = merge_res.row_count_;
      create_param.column_cnt_ = merge_res.data_column_cnt_;
      create_param.data_checksum_ = merge_res.data_checksum_;
      create_param.occupy_size_ = merge_res.occupy_size_;
      create_param.original_size_ = merge_res.original_size_;
      create_param.max_merged_trans_version_ = merge_res.max_merged_trans_version_;
      create_param.contain_uncommitted_row_ = merge_res.contain_uncommitted_row_;
      create_param.compressor_type_ = merge_res.compressor_type_;
      create_param.encrypt_id_ = merge_res.encrypt_id_;
      create_param.master_key_id_ = merge_res.master_key_id_;
      create_param.data_block_ids_ = merge_res.data_block_ids_;
      create_param.other_block_ids_ = merge_res.other_block_ids_;
      MEMCPY(create_param.encrypt_key_, merge_res.encrypt_key_,
             OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
      if (OB_FAIL(
            merge_res.fill_column_checksum(&storage_schema, create_param.column_checksums_))) {
        LOG_WARN("fail to fill column checksum for empty major", KR(ret), K(create_param));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_param, table_handle))) {
        LOG_WARN("fail to create sstable", KR(ret), K(create_param));
      } else {
        const int64_t rebuild_seq = ls_handle_.get_ls()->get_rebuild_seq();
        ObTabletHandle new_tablet_handle;
        ObUpdateTableStoreParam table_store_param(table_handle,
                                                  tablet_handle_.get_obj()->get_snapshot_version(),
                                                  false, &storage_schema, rebuild_seq, true, true);
        // 把表更新到对应的存储队列里去 
        if (OB_FAIL(ls_handle_.get_ls()->update_tablet_table_store(tablet_id_, table_store_param,
                                                                   new_tablet_handle))) {
          LOG_WARN("fail to update tablet table store", KR(ret), K(tablet_id_),
                   K(table_store_param));
        }
      }
    }
  }
  return ret;
}

int ObLoadSSTableWriter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed sstable writer", KR(ret));
  } else {
    ObSSTable *sstable = nullptr;
    if (OB_FAIL(macro_block_writer_.close())) {
      LOG_WARN("fail to close macro block writer", KR(ret));
    } else if (OB_FAIL(create_sstable())) {
      LOG_WARN("fail to create sstable", KR(ret));
    } else {
      is_closed_ = true;
    }
  }
  return ret;
}

/**
 * ObLoadDataDirectDemo
 */

ObLoadDataDirectDemo::ObLoadDataDirectDemo()
{
}

ObLoadDataDirectDemo::~ObLoadDataDirectDemo()
{
}

int ObLoadDataDirectDemo::execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(load_stmt))) {   // 初始化操作
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_FAIL(do_load(ctx, load_stmt))) {
    LOG_WARN("fail to do load", KR(ret));
  }
  return ret;
}

int ObLoadDataDirectDemo::inner_init(ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list =
    load_stmt.get_field_or_var_list();
  const uint64_t tenant_id = load_args.tenant_id_;
  const uint64_t table_id = load_args.table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id,
                                                                                  schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_UNLIKELY(table_schema->is_heap_table())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support heap table", KR(ret));
  }
  // init csv_parser_
  else if (OB_FAIL(csv_parser_.init(load_stmt.get_data_struct_in_file(), field_or_var_list.count(),
                                    load_args.file_cs_type_))) {
    LOG_WARN("fail to init csv parser", KR(ret));
  }
  // init file_reader_
  else if (OB_FAIL(file_reader_.open(load_args.full_file_path_))) {
    LOG_WARN("fail to open file", KR(ret), K(load_args.full_file_path_));
  }
  // init buffer_
  else if (OB_FAIL(buffer_.create(FILE_BUFFER_SIZE))) {
    LOG_WARN("fail to create buffer", KR(ret));
  }
  // init row_caster_
  else if (OB_FAIL(row_caster_.init(table_schema, field_or_var_list))) {
    LOG_WARN("fail to init row caster", KR(ret));
  }
  // init external_sort_
  else if (OB_FAIL(external_sort_.init(table_schema, MEM_BUFFER_SIZE, FILE_BUFFER_SIZE))) {
    LOG_WARN("fail to init row caster", KR(ret));
  }
  // init sstable_writer_
  else if (OB_FAIL(sstable_writer_.init(table_schema))) {
    LOG_WARN("fail to init sstable writer", KR(ret));
  }
  return ret;
}

std::mutex mtx;
// void thread_read_buffer(void *arg, ObLoadCSVPaser *csv_parser_i, ObLoadRowCaster *row_caster_i, ObLoadExternalSort *external_sort_i)
void thread_read_buffer(void *arg)
{
  #if 0   // 输出成文件
	is.seekg(start_point);
	std::string url;
  std::vector<std::string> vec = {"thread-0: ", "thread-1: ", "thread-2: ", "thread-3: ", "thread-4: ", "thread-5: ", "thread-6: ", "thread-7: "};
	while(volume > 0 && getline(is, url)) {
    // url = vec[id] + url + "\n";
    out << url;
		volume -= url.size() + 1;

		// if((volume & 1048575) == 1048575) printf("%d %lld\n", id, volume);
		++cnt[id];
	}
  #endif

  int ret = OB_SUCCESS;
  const ObNewRow *new_row = nullptr;
  const ObLoadDatumRow *datum_row = nullptr;
  Task *task = (Task *)arg;
  ObLoadDataDirectDemo *this_ = task->_this_;
  ObLoadDataBuffer *buffer_i = task->buffer_i_;
  ObLoadCSVPaser *csv_parser_i = task->csv_parser_i_;
  ObLoadRowCaster *row_caster_i = task->row_caster_i_;
  int a = 0;
  while (OB_SUCC(ret)) {
    a++;
    // 笔记：csv_parser_不能公用，得一个buffer一个csv_parser, row_caster_同理.
    if (OB_FAIL(csv_parser_i->get_next_row(*buffer_i, new_row))) {   
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(row_caster_i->get_casted_row(*new_row, datum_row))) {   
      LOG_WARN("fail to cast row", KR(ret));
    } else {
      mtx.lock();
      // int64_t begin_pos = buffer_i->begin_pos();
      // char *str = buffer_i->begin();
      ret = this_->external_sort_.append_row(*datum_row);
      if (ret != OB_SUCCESS)
        LOG_INFO("liangman", KR(a), KR(buffer_i->threadID()));
      mtx.unlock();
    } 
    // else if (OB_FAIL(external_sort_i->append_row(*datum_row))) {  // append_row()若用公用的this_->external_sort_，有问题，暂未解决
    //   LOG_WARN("fail to append row", KR(ret));
    // }
    /*else {
      // 写入读取到的每一行记录到文件里
      for (int i = 0; i < new_row->count_; ++i) {
        const char *str = new_row->cells_[i].get_string_ptr();
        int len = new_row->cells_[i].get_string_len();
        if (i != 0) {
          char ch = '|';
          out.write(&ch, 1);
        }
        if(out && out.is_open()) {
          for (int j = 0; j < len; ++j)
            out.write(str + j, 1);
        }
      }
      char ch = '\n';
      out.write(&ch, 1);
    }*/
  }
}

std::vector<std::pair<int64_t, int64_t>> get_read_pos(ObLoadDataBuffer *buffer, int threads, int64_t length, std::istringstream &is)
{
  std::vector<std::pair<int64_t, int64_t>> res(threads);
  std::string buf_string(buffer->data(), buffer->get_data_size());
  is.str(buf_string);
	res[0].first = 0;
	for(int i = 1; i < threads; ++i) {
		is.seekg(length/threads * i);   // seekg(val),从第val个字节之后开始读，不读第val个字节
		{
			std::string tmp; 
			getline(is, tmp);
		}
		res[i].first = is.tellg();
		res[i-1].second = res[i].first - res[i-1].first;
	}
	res.back().second = length - res.back().first;
	
	for (int i = 0; i < res.size(); ++i)
		printf("thread-%d start: %ld, end: %ld, size: %ld\n", i, res[i].first, res[i].first + res[i].second, res[i].second);
	
	return res;
}

int ObLoadDataDirectDemo::do_load(ObExecContext &ctx, ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  const ObNewRow *new_row = nullptr;
  const ObLoadDatumRow *datum_row = nullptr;

  const int threads = 7;    // 7个子线程用于并行解析buffer_里的数据(消费者), 一个主线程用于读取磁盘里的数据2M存储到buffer_里(生产者)

  // std::ofstream out("/root/1out.csv");

  // 初始化
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list = load_stmt.get_field_or_var_list(); 
  const uint64_t tenant_id = load_args.tenant_id_;
  const uint64_t table_id = load_args.table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard);
  schema_guard.get_table_schema(tenant_id, table_id, table_schema);
  ObLoadDataBuffer buffer_i[threads];     // buffer_i是分配的动态内存，最后需要手动释放
  ObLoadCSVPaser csv_parser_i[threads];   // csv_parser_i也是动态分配的内存，到init()函数内看到
  ObLoadRowCaster row_caster_i[threads];
  // ObLoadExternalSort external_sort_i[threads];
  for (int i = 0; i < threads; ++i) {
    // 初始化buffer
    buffer_i[i].create(FILE_BUFFER_SIZE);
    // 初始化csv_parser
    csv_parser_i[i].init(load_stmt.get_data_struct_in_file(), field_or_var_list.count(), load_args.file_cs_type_);
    // 初始化row_caster
    row_caster_i[i].init(table_schema, field_or_var_list);
    // 初始化external_sort_
    // external_sort_i[i].init(table_schema, MEM_BUFFER_SIZE, FILE_BUFFER_SIZE);
  }

  // 创建线程池
  MyThreadPool thread_pool(threads);
  thread_pool.set_thread_count(threads);
  thread_pool.set_run_wrapper(MTL_CTX());
  thread_pool.start();
  // thread_pool.init(load_stmt);
  // thread_pool.createPool();

  // LOG_INFO("liangman: start in 1135");
  while (OB_SUCC(ret)) {   // 条件为假，表示整个文件数据都读取完了
    if (OB_FAIL(buffer_.squash())) {    
        LOG_WARN("fail to squash buffer", KR(ret));
    } else if (OB_FAIL(file_reader_.read_next_buffer(buffer_))) {  
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to read next buffer", KR(ret));
      } else {
        if (OB_UNLIKELY(!buffer_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected incomplate data", KR(ret));
        }
        ret = OB_SUCCESS;
        break;   
      }
    } else if (OB_UNLIKELY(buffer_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty buffer", KR(ret));
    } else {

      #if 0
      // 多线程读取buffer_中的数据
      // 现在buffer_里存储着小于等于2M的数据，先获取buffer_实际字节大小
      int64_t buffer_size = buffer_.get_data_size();
      // 给每个线程划分要读取的数据范围：起始点，终止点
      std::istringstream is;
      std::vector<std::pair<int64_t, int64_t>> read_pos = get_read_pos(&buffer_, threads, buffer_size, is);
      // 将buffer_一分为8
      // 在每个线程内，各自对数据进行get_next_row和get_casted_row操作
      std::vector<std::thread> vec_thread;
      for(int i = 0; i < threads; ++i) {
        buffer_i[i].set_data(buffer_.data());
        buffer_i[i].set_begin(read_pos[i].first);
        buffer_i[i].set_end(read_pos[i].first + read_pos[i].second);
        buffer_i[i].set_threadID(i);
      
        // 笔记：①通过this指针传递调用当前成员函数的类的对象. ②创建线程时，传递的参数为引用时要加std::ref().
        // std::thread th(thread_read_buffer, i, read_pos[i].first, read_pos[i].second, std::ref(is), std::ref(out), this, &buffer_i[i], &csv_parser_i[i], &row_caster_i[i]);
        std::thread th(thread_read_buffer, this, &buffer_i[i], &csv_parser_i[i], &row_caster_i[i]);
        vec_thread.push_back(std::move(th));
      }
      for(auto &th : vec_thread)
        th.join();

      buffer_.set_begin(buffer_i[threads-1].begin_pos());
      buffer_.set_end(buffer_i[threads-1].end_pos());
      // 每个线程将最终得到的datum_row写入external_sort_，得加锁
      // 注：虽然往external_sort_内写入记录的顺序改变了，但是存满1G后都要排序，所以写入顺序不是buffer_里原来的顺序，也没问题
      #endif

      #if 0
      // LOG_INFO("liangman: start in 1184");
      while (OB_SUCC(ret)) {
        if (OB_FAIL(csv_parser_.get_next_row(buffer_, new_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(row_caster_.get_casted_row(*new_row, datum_row))) {
          LOG_WARN("fail to cast row", KR(ret));
        } else if (OB_FAIL(external_sort_.append_row(*datum_row))) {
          LOG_WARN("fail to append row", KR(ret));
        }
      }
      // LOG_INFO("liangman: end in 1199");
      #endif

      #if 1
      // LOG_INFO("liangman: start in 1204");
      // 多线程读取buffer_中的数据
      // 现在buffer_里存储着小于等于2M的数据，先获取buffer_实际字节大小
      // LOG_INFO("liangman: start in 1206");
      int64_t buffer_size = buffer_.get_data_size();
      // 给每个线程划分要读取的数据范围：起始点，终止点
      std::istringstream is;
      std::vector<std::pair<int64_t, int64_t>> read_pos = get_read_pos(&buffer_, threads, buffer_size, is);
      // LOG_INFO("liangman: end in 1211");
      // 将buffer_一分为8
      // 在每个线程内，各自对数据进行get_next_row和get_casted_row操作
      std::vector<std::thread> vec_thread;
      // 创建任务
      thread_pool.count_ = 0;
      for (int i = 0; i < threads; ++i) {
        buffer_i[i].set_data(buffer_.data());
        buffer_i[i].set_begin(read_pos[i].first);
        buffer_i[i].set_end(read_pos[i].first + read_pos[i].second);
        buffer_i[i].set_threadID(i);
        // thread_pool.push_task(&thread_read_buffer, &external_sort_i[i], &buffer_i[i], &csv_parser_i[i], &row_caster_i[i]);
        thread_pool.push_task(&thread_read_buffer, this, &buffer_i[i], &csv_parser_i[i], &row_caster_i[i]);
      }
      while (true) {
        if (thread_pool.count_ == threads)
          break;
      }
      buffer_.set_begin(buffer_i[threads-1].begin_pos());
      buffer_.set_end(buffer_i[threads-1].end_pos());
      // LOG_INFO("liangman: end in 1230");

      #endif

      #if 0
      thread_pool.push_task(&thread_read_buffer, &buffer_);    
      while (true) {
        if (thread_pool.task_queue_.size() < 15)   // 等待任务队列中任务数小于15个，才继续push新的任务
          break;
      }
      #endif
    }   
  }
  // out.close();
  // 释放buffer_i、csv_parser_i、row_caster_i    TODO

  // 对读取的所有数据进行外部排序
  if (OB_SUCC(ret)) {
    if (OB_FAIL(external_sort_.close())) {   // 进行merge_sort排序
      LOG_WARN("fail to close external sort", KR(ret));
    }
    // for (int i = 0; i < threads; ++i)
    //   external_sort_i[i].close();
  }
  // 将排序好的记录，存储为SSTable
  while (OB_SUCC(ret)) {      // 有多少行记录，循环多少次
    if (OB_FAIL(external_sort_.get_next_row(datum_row))) {    
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(sstable_writer_.append_row(*datum_row))) {  
      LOG_WARN("fail to append row", KR(ret));
    }
  }
  // close()就是把内存中的数据刷到宏块上，同时把刷出来的宏快丢给sstable_index_build去，之后就是构造sstable了
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_writer_.close())) {
      LOG_WARN("fail to close sstable writer", KR(ret));
    }
  }
  return ret;
}

// 其代表了每个线程池的线程始终在跑的循环，在无任务分配的时候阻塞在某个位置。
void *WorkThread::start(void *arg) 
{
  //-获得执行对象
  WorkThread *ee = (WorkThread *)arg;
  while(true) {
    //-加锁
    pthread_mutex_lock(&(ee->pool_->mutex_));
    while(ee->pool_->task_queue_.empty()) { //-如果任务队列为空，等待新任务
      if(!ee->usable_) {
        break;
      }
      pthread_cond_wait(&ee->pool_->cont_, &ee->pool_->mutex_);
    }
    if(!ee->usable_) {
      pthread_mutex_unlock(&ee->pool_->mutex_);
      break;
    }
    Task *task = ee->pool_->task_queue_.front();
    ee->pool_->task_queue_.pop_front();
    int pid = ee->pid_;
    // ObLoadCSVPaser *csv_parser = &(ee->pool_->csv_parser_i_[pid]);
    // ObLoadRowCaster *row_caster = &(ee->pool_->row_caster_i_[pid]);
    // ObLoadExternalSort *external_sort = &(ee->pool_->external_sort_i_[pid]);
    //-解锁
    pthread_mutex_unlock(&(ee->pool_->mutex_));
    //-执行任务回调
    task->task_call_back(task);  
    // task->task_call_back(task, csv_parser, row_caster, external_sort);

    pthread_mutex_lock(&(ee->pool_->mutex_));
    ee->pool_->count_++;
    pthread_mutex_unlock(&(ee->pool_->mutex_));
  }
  //-删除线程执行对象
  delete ee;

  return nullptr;
}

void MyThreadPool::run1()
{
  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  ObTenantBase *tenant_base = MTL_CTX();
  Worker::CompatMode mode = ((ObTenant *)tenant_base)->get_compat_mode();
  Worker::set_compatibility_mode(mode);

  while(true) {
    //-加锁
    pthread_mutex_lock(&mutex_);
    while(task_queue_.empty()) { //-如果任务队列为空，等待新任务
      if(!usable_) {
        break;
      }
      pthread_cond_wait(&cont_, &mutex_);
    }
    if(!usable_) {
      pthread_mutex_unlock(&mutex_);
      break;
    }
    Task *task = task_queue_.front();
    task_queue_.pop_front();
    // int pid = ee->pid_;
    // ObLoadCSVPaser *csv_parser = &(ee->pool_->csv_parser_i_[pid]);
    // ObLoadRowCaster *row_caster = &(ee->pool_->row_caster_i_[pid]);
    // ObLoadExternalSort *external_sort = &(ee->pool_->external_sort_i_[pid]);
    //-解锁
    pthread_mutex_unlock(&mutex_);
    //-执行任务回调
    task->task_call_back(task);  
    // task->task_call_back(task, csv_parser, row_caster, external_sort);

    pthread_mutex_lock(&mutex_);
    count_++;
    pthread_mutex_unlock(&mutex_);
  }
}

void MyThreadPool::createPool() 
{
  //-初始执行队列
  for(int i = 0; i < thread_count_; ++i) {
    WorkThread *ee = new WorkThread;
    ee->pool_ = const_cast<MyThreadPool *>(this);
    ee->pid_ = i;
    pthread_create(&(ee->tid_), NULL, ee->start, ee);
    work_thread_queue_.push_back(ee);
  }
}

// 传入csv_parser_i数组，具体选哪个,由被唤醒的线程pid去选择
// 因此Task里属性就不能是单个csv_parser_i，而是一个数组了
void MyThreadPool::push_task(void(* tcb)(void *), 
                              ObLoadDataDirectDemo *this_, 
                              ObLoadDataBuffer *buffer_i, 
                              ObLoadCSVPaser *csv_parser_i, 
                              ObLoadRowCaster *row_caster_i) 
// void MyThreadPool::push_task(void(* tcb)(void *, ObLoadCSVPaser *, ObLoadRowCaster *, ObLoadExternalSort *), ObLoadDataBuffer *buffer_i)
{
  Task *task = new Task;
  task->setFunc(tcb);
  task->_this_ = this_;
  task->buffer_i_ = buffer_i;
  task->csv_parser_i_ = csv_parser_i;
  task->row_caster_i_ = row_caster_i;
  // task->buffer = buffer_i;
  //-加锁
  pthread_mutex_lock(&mutex_);
  task_queue_.push_back(task);
  //-通知执行队列中的一个进行任务
  pthread_cond_signal(&cont_);
  //-解锁
  pthread_mutex_unlock(&mutex_);
}

int MyThreadPool::init(ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  #if 0
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list = load_stmt.get_field_or_var_list(); 
  const uint64_t tenant_id = load_args.tenant_id_;
  const uint64_t table_id = load_args.table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard);
  schema_guard.get_table_schema(tenant_id, table_id, table_schema);
  for (int i = 0; i < thread_count_; ++i) {
    // 初始化csv_parser
    csv_parser_i_[i].init(load_stmt.get_data_struct_in_file(), field_or_var_list.count(), load_args.file_cs_type_);
    // 初始化row_caster
    row_caster_i_[i].init(table_schema, field_or_var_list);
    // 初始化external_sort_
    external_sort_i_[i].init(table_schema, MEM_BUFFER_SIZE, FILE_BUFFER_SIZE);
  }
  #endif
  return ret;
}

// MyThreadPool::MyThreadPool(int thread_count, ObLoadDataStmt &load_stmt)
MyThreadPool::MyThreadPool(int thread_count)
{
  thread_count_ = thread_count;
  count_ = 0;
  pthread_cond_init(&cont_, nullptr);
  pthread_mutex_init(&mutex_, nullptr);
}

MyThreadPool::~MyThreadPool()
{
  for (int i = 0; i < work_thread_queue_.size(); ++i) {
    work_thread_queue_[i]->usable_ = false;
  }
  usable_ = false;
  pthread_mutex_lock(&mutex_);
  //-清空任务队列
  task_queue_.clear();
  //-广播给每个执行线程令其退出(执行线程破开循环会free掉堆内存)
  pthread_cond_broadcast(&cont_);
  pthread_mutex_unlock(&mutex_);  //-让其他线程拿到锁
  //-等待所有线程退出
  // for (int i = 0; i < work_thread_queue_.size(); ++i) {
  //   pthread_join(work_thread_queue_[i]->tid_, NULL);
  // }
  //-清空执行队列
  work_thread_queue_.clear();
  //-销毁锁和条件变量
  pthread_cond_destroy(&cont_);
  pthread_mutex_destroy(&mutex_);
}

} // namespace sql
} // namespace oceanbase