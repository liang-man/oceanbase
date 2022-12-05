/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_BLOCKSSTABLE_DATUM_ROW_H
#define OB_STORAGE_BLOCKSSTABLE_DATUM_ROW_H

#include "common/ob_common_types.h"
#include "common/ob_tablet_id.h"
#include "share/datum/ob_datum.h"
#include "share/datum/ob_datum_funcs.h"
#include "storage/tx/ob_trans_define.h"
#include "common/row/ob_row.h"
#include "storage/ob_storage_util.h"

namespace oceanbase
{
namespace share{
namespace schema
{
struct ObColDesc;
}
}
namespace storage
{
struct ObStoreRow;
}
namespace blocksstable
{

struct ObDmlRowFlag;

enum ObDmlFlag
{
  DF_NOT_EXIST = 0,
  DF_LOCK = 1,
  DF_UPDATE = 2,
  DF_INSERT = 3,
  DF_DELETE = 4,
  DF_MAX = 5,
};

static const char *ObDmlFlagStr[DF_MAX] = {
    "NOT_EXIST",
    "LOCK",
    "UPDATE",
    "INSERT",
    "DELETE"
};

enum ObDmlRowFlagType
{
  DF_TYPE_NORMAL = 0,
  DF_TYPE_INSERT_DELETE = 1,
  DF_TYPE_MAX,
};

static const char *ObDmlTypeStr[DF_TYPE_MAX] = {
    "N",
    "I_D"
};

const char *get_dml_str(ObDmlFlag dml_flag);
void format_dml_str(const int32_t flag, char *str, int len);

struct ObDmlRowFlag
{
  OB_UNIS_VERSION(1);
public:
  ObDmlRowFlag()
   : whole_flag_(0)
  {
  }
  ObDmlRowFlag(const uint8_t flag)
   : whole_flag_(flag)
  {
  }
  ObDmlRowFlag(ObDmlFlag flag)
   : whole_flag_(0)
  {
    set_flag(flag);
  }
  ~ObDmlRowFlag()
  {
  }
  OB_INLINE void reset()
  {
    whole_flag_ = 0;
  }
  OB_INLINE void set_flag(ObDmlFlag row_flag)
  {
    if (OB_LIKELY(row_flag >= DF_NOT_EXIST && row_flag < DF_MAX)) {
      flag_ = row_flag;
    }
  }
  OB_INLINE bool is_delete() const
  {
    return DF_DELETE == flag_;
  }
  OB_INLINE bool is_lock() const
  {
    return DF_LOCK == flag_;
  }
  OB_INLINE bool is_not_exist() const
  {
    return DF_NOT_EXIST == flag_;
  }
  OB_INLINE bool is_insert() const
  {
    return DF_INSERT == flag_;
  }
  OB_INLINE bool is_update() const
  {
    return DF_UPDATE == flag_;
  }
  OB_INLINE bool is_exist() const
  {
    return is_valid() && !is_not_exist();
  }
  OB_INLINE bool is_exist_without_delete() const
  {
    return is_exist() && !is_delete();
  }
  OB_INLINE bool is_valid() const
  {
    return (DF_TYPE_NORMAL == flag_type_ && DF_DELETE >= flag_)
        || (DF_TYPE_INSERT_DELETE == flag_type_ && DF_DELETE == flag_);
  }
  OB_INLINE bool is_extra_delete() const
  {
    return DF_TYPE_INSERT_DELETE != flag_type_ && DF_DELETE == flag_;
  }
  OB_INLINE bool is_insert_delete() const
  {
    return DF_TYPE_INSERT_DELETE == flag_type_ && DF_DELETE == flag_;
  }
  OB_INLINE void fuse_flag(const ObDmlRowFlag input_flag)
  {
    if (OB_LIKELY(input_flag.is_valid())) {
      if (DF_INSERT == input_flag.flag_) {
        if (DF_DELETE == flag_) {
          flag_type_ = DF_TYPE_INSERT_DELETE;
        } else {
          flag_ = DF_INSERT;
        }
      } else if (DF_DELETE == input_flag.flag_ && DF_DELETE == flag_) {
        if (flag_type_ == DF_TYPE_INSERT_DELETE) {
          flag_type_ = input_flag.flag_type_;
        } else {
          STORAGE_LOG(DEBUG, "unexpected pure delete row", KPC(this), K(input_flag));
        }
      }
    }
  }
  OB_INLINE uint8_t get_serialize_flag() const // use when Serialize or print
  {
    return whole_flag_;
  }
  OB_INLINE ObDmlFlag get_dml_flag() const { return (ObDmlFlag)flag_; }
  ObDmlRowFlag & operator = (const ObDmlRowFlag &other)
  {
    if (other.is_valid()) {
      whole_flag_ = other.whole_flag_;
    }
    return *this;
  }

  const char *getFlagStr() const
  {
    const char *ret_str = nullptr;
    if (is_valid()) {
      ret_str = ObDmlFlagStr[flag_];
    } else {
      ret_str = "invalid flag";
    }
    return ret_str;
  }
  OB_INLINE void format_str(char *str, int8_t len) const
  { return format_dml_str(whole_flag_, str, len); }

  TO_STRING_KV("flag", get_dml_str(ObDmlFlag(flag_)), K_(flag_type)) ;
private:
  bool operator !=(const ObDmlRowFlag &other) const // for unittest
  {
    return flag_ != other.flag_;
  }

  const static uint8_t OB_FLAG_TYPE_MASK = 0x80;
  const static uint8_t OB_FLAG_MASK = 0x7F;
  union
  {
    uint8_t whole_flag_;
    struct {
      uint8_t flag_      : 7;  // store ObDmlFlag
      uint8_t flag_type_ : 1;  // mark is pure_delete or insert_delete
    };
  };
};

static const int8_t MvccFlagCount = 8;
static const char *ObMvccFlagStr[MvccFlagCount] = {
  "",
  "F",
  "U",
  "S",
  "C",
  "G",
  "L",
  "UNKNOWN"
};

void format_mvcc_str(const int32_t flag, char *str, int len);

struct ObMultiVersionRowFlag
{
  OB_UNIS_VERSION(1);
public:
  union
  {
    uint8_t flag_;
    struct
    {
      uint8_t is_first_        : 1;    // 0: not first row(default), 1: first row
      uint8_t is_uncommitted_  : 1;    // 0: committed(default), 1: uncommitted row
      uint8_t is_shadow_       : 1;    // 0: not new compacted shadow row(default), 1: shadow row
      uint8_t is_compacted_    : 1;    // 0: multi_version_row(default), 1: compacted_multi_version_row
      uint8_t is_ghost_        : 1;    // 0: not ghost row(default), 1: ghost row
      uint8_t is_last_         : 1;    // 0: not last row(default), 1: last row
      uint8_t reserved_        : 2;
    };
  };

  ObMultiVersionRowFlag() : flag_(0) {}
  ObMultiVersionRowFlag(uint8_t flag) : flag_(flag) {}
  void reset() { flag_ = 0; }
  inline void set_compacted_multi_version_row(const bool is_compacted_multi_version_row)
  {
    is_compacted_ = is_compacted_multi_version_row;
  }
  inline void set_last_multi_version_row(const bool is_last_multi_version_row)
  {
    is_last_ = is_last_multi_version_row;
  }
  inline void set_first_multi_version_row(const bool is_first_multi_version_row)
  {
    is_first_ = is_first_multi_version_row;
  }
  inline void set_uncommitted_row(const bool is_uncommitted_row)
  {
    is_uncommitted_ = is_uncommitted_row;
  }
  inline void set_ghost_row(const bool is_ghost_row)
  {
    is_ghost_ = is_ghost_row;
  }
  inline void set_shadow_row(const bool is_shadow_row)
  {
    is_shadow_ = is_shadow_row;
  }
  inline bool is_compacted_multi_version_row() const { return is_compacted_; }
  inline bool is_last_multi_version_row() const { return is_last_; }
  inline bool is_first_multi_version_row() const { return is_first_; }
  inline bool is_uncommitted_row() const { return is_uncommitted_; }
  inline bool is_ghost_row() const { return is_ghost_; }
  inline bool is_shadow_row() const { return is_shadow_; }
  inline void format_str(char *str, int8_t len) const
  { return format_mvcc_str(flag_, str, len); }

  TO_STRING_KV("first", is_first_,
               "uncommitted", is_uncommitted_,
               "shadow", is_shadow_,
               "compact", is_compacted_,
               "ghost", is_ghost_,
               "last", is_last_,
               "reserved", reserved_,
               K_(flag));
};

//TODO optimize number buffer
struct ObStorageDatum : public common::ObDatum
{
  ObStorageDatum() { set_nop(); }
  ObStorageDatum(const ObStorageDatum &datum) { reuse(); *this = datum; }

  ~ObStorageDatum() = default;
  // ext value section
  OB_INLINE void reuse() { ptr_ = buf_; reserved_ = 0; pack_ = 0; }
  OB_INLINE void set_ext_value(const int64_t ext_value)
  { reuse(); set_ext(); no_cv(extend_obj_)->set_ext(ext_value); }
  OB_INLINE void set_nop() { set_ext_value(ObActionFlag::OP_NOP); }
  OB_INLINE void set_min() { set_ext_value(common::ObObj::MIN_OBJECT_VALUE); }
  OB_INLINE void set_max() { set_ext_value(common::ObObj::MAX_OBJECT_VALUE); }
  OB_INLINE bool is_nop_value() const { return is_nop(); } // temp solution
  // transfer section
  OB_INLINE bool is_local_buf() const { return ptr_ == buf_; }
  OB_INLINE int from_buf_enhance(const char *buf, const int64_t buf_len);
  OB_INLINE int from_obj_enhance(const common::ObObj &obj);
  OB_INLINE int my_from_obj_enhance(const common::ObObj &obj);   // liangman
  OB_INLINE int to_obj_enhance(common::ObObj &obj, const common::ObObjMeta &meta) const;
  OB_INLINE int deep_copy(const ObStorageDatum &src, common::ObIAllocator &allocator);
  OB_INLINE int deep_copy(const ObStorageDatum &src, char * buf, const int64_t buf_len, int64_t &pos);
  OB_INLINE int64_t get_deep_copy_size() const;
  OB_INLINE ObStorageDatum& operator=(const ObStorageDatum &other);
  OB_INLINE int64_t storage_to_string(char *buf, int64_t buf_len) const;
  //only for unittest
  OB_INLINE bool operator==(const ObStorageDatum &other) const;
  OB_INLINE bool operator==(const ObObj &other) const;

  //datum 12 byte
  int32_t reserved_;
  // buf 16 byte
  char buf_[common::OBJ_DATUM_NUMBER_RES_SIZE];
};

struct ObStorageDatumBuffer
{
public:
  ObStorageDatumBuffer(common::ObIAllocator *allocator = nullptr);
  ~ObStorageDatumBuffer() { reset(); }
  void reset();
  int init(common::ObIAllocator &allocator);
  int reserve(const int64_t count, const bool keep_data = false);
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE ObStorageDatum *get_datums() { return datums_; }
  OB_INLINE int64_t get_capacity() const { return capacity_; }
  TO_STRING_KV(K_(capacity), KP_(datums), KP_(local_datums));
private:
  static const int64_t LOCAL_BUFFER_ARRAY = common::OB_ROW_DEFAULT_COLUMNS_COUNT;
  int64_t capacity_;
  ObStorageDatum local_datums_[LOCAL_BUFFER_ARRAY];
  ObStorageDatum *datums_;
  common::ObIAllocator *allocator_;
  bool is_inited_;
};

struct ObDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObDatumRow();
  ~ObDatumRow();
  int init(common::ObIAllocator &allocator, const int64_t capacity);
  int init(const int64_t capacity);
  void reset();
  void reuse();
  int reserve(const int64_t capacity, const bool keep_data = false);
  int deep_copy(const ObDatumRow &src, common::ObIAllocator &allocator);
  //TODO need remove by @hanhui
  int prepare_new_row(const common::ObIArray<share::schema::ObColDesc> &out_cols);
  int to_store_row(const common::ObIArray<share::schema::ObColDesc> &out_cols, storage::ObStoreRow &store_row);
  int from_store_row(const storage::ObStoreRow &store_row);
  //only for unittest
  bool operator==(const ObDatumRow &other) const;
  bool operator==(const common::ObNewRow &other) const;

  OB_INLINE ObNewRow &get_new_row() { return old_row_; }
  OB_INLINE const ObNewRow &get_new_row() const { return old_row_; }
  OB_INLINE int64_t get_capacity() const { return datum_buffer_.get_capacity(); }
  OB_INLINE int64_t get_column_count() const { return count_; }
  OB_INLINE bool is_valid() const { return nullptr != storage_datums_ && get_capacity() > 0; }
  /*
   *multi version row section
   */
  OB_INLINE transaction::ObTransID get_trans_id() const { return trans_id_; }
  OB_INLINE void set_trans_id(const transaction::ObTransID &trans_id) { trans_id_ = trans_id; }
  OB_INLINE bool is_have_uncommited_row() const { return have_uncommited_row_; }
  OB_INLINE void set_have_uncommited_row(const bool have_uncommited_row = true) { have_uncommited_row_ = have_uncommited_row; }
  OB_INLINE bool is_ghost_row() const { return mvcc_row_flag_.is_ghost_row(); }
  OB_INLINE bool is_uncommitted_row() const { return mvcc_row_flag_.is_uncommitted_row(); }
  OB_INLINE bool is_compacted_multi_version_row() const { return mvcc_row_flag_.is_compacted_multi_version_row(); }
  OB_INLINE bool is_first_multi_version_row() const { return mvcc_row_flag_.is_first_multi_version_row(); }
  OB_INLINE bool is_last_multi_version_row() const { return mvcc_row_flag_.is_last_multi_version_row(); }
  OB_INLINE bool is_shadow_row() const { return mvcc_row_flag_.is_shadow_row(); }
  OB_INLINE void set_compacted_multi_version_row() { mvcc_row_flag_.set_compacted_multi_version_row(true); }
  OB_INLINE void set_first_multi_version_row() { mvcc_row_flag_.set_first_multi_version_row(true); }
  OB_INLINE void set_last_multi_version_row() { mvcc_row_flag_.set_last_multi_version_row(true); }
  OB_INLINE void set_shadow_row() { mvcc_row_flag_.set_shadow_row(true); }
  OB_INLINE void set_multi_version_flag(const ObMultiVersionRowFlag &multi_version_flag) { mvcc_row_flag_ = multi_version_flag; }
  /*
   *row estimate section
   */
  OB_INLINE int32_t get_delta() const;

  DECLARE_TO_STRING;

public:
  common::ObArenaAllocator local_allocator_;
  uint16_t count_;
  bool fast_filter_skipped_;
  bool have_uncommited_row_;
  ObDmlRowFlag row_flag_;
  ObMultiVersionRowFlag mvcc_row_flag_;
  transaction::ObTransID trans_id_;
  int64_t scan_index_;
  int64_t group_idx_;
  int64_t snapshot_version_;
  ObStorageDatum *storage_datums_;
  // do not need serialize
  ObStorageDatumBuffer datum_buffer_;
  //TODO @hanhui only for compile
  common::ObNewRow old_row_;
  storage::ObObjBufArray obj_buf_;
};

struct ObConstDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObConstDatumRow() { MEMSET(this, 0, sizeof(ObConstDatumRow)); }
  ObConstDatumRow(const ObDatum *datums, uint64_t count)
    : datums_(datums), count_(count) {}
  ~ObConstDatumRow() {}
  OB_INLINE int64_t get_column_count() const { return count_; }
  OB_INLINE bool is_valid() const { return nullptr != datums_ && count_ > 0; }
  OB_INLINE const ObDatum &get_datum(const int64_t col_idx) const
  {
    OB_ASSERT(col_idx < count_ && col_idx >= 0);
    return datums_[col_idx];
  }
  TO_STRING_KV(K_(count), "datums_:", ObArrayWrap<ObDatum>(datums_, count_));
  const ObDatum *datums_;
  uint64_t count_;
};

struct ObStorageDatumCmpFunc
{
public:
  ObStorageDatumCmpFunc(common::ObCmpFunc &cmp_func) : cmp_func_(cmp_func) {}
  ObStorageDatumCmpFunc() = default;
  ~ObStorageDatumCmpFunc() = default;
  int compare(const ObStorageDatum &left, const ObStorageDatum &right, int &cmp_ret) const;
  TO_STRING_KV(K_(cmp_func));
private:
  common::ObCmpFunc cmp_func_;
};

typedef common::ObFixedArray<ObStorageDatumCmpFunc, common::ObIAllocator> ObStoreCmpFuncs;
struct ObStorageDatumUtils
{
public:
  ObStorageDatumUtils();
  ~ObStorageDatumUtils();
  int init(const common::ObIArray<share::schema::ObColDesc> &col_descs,
           const int64_t schema_rowkey_cnt,
           const bool is_oracle_mode,
           common::ObIAllocator &allocator);
  void reset();
  OB_INLINE bool is_valid() const { return is_inited_; }
  OB_INLINE bool is_oracle_mode() const { return is_oracle_mode_; }
  OB_INLINE int64_t get_rowkey_count() const { return rowkey_cnt_; }
  OB_INLINE int64_t get_column_count() const { return col_cnt_; }
  OB_INLINE const ObStoreCmpFuncs &get_cmp_funcs() const { return cmp_funcs_; }
  OB_INLINE const common::ObHashFuncs &get_hash_funcs() const { return hash_funcs_; }
  OB_INLINE const common::ObHashFunc &get_ext_hash_funcs() const { return ext_hash_func_; }
  TO_STRING_KV(K_(is_oracle_mode), K_(rowkey_cnt), K_(col_cnt), KP_(allocator), K_(is_inited));
private:
  //TODO to be removed by @hanhui
  int transform_multi_version_col_desc(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                       const int64_t schema_rowkey_cnt,
                                       common::ObIArray<share::schema::ObColDesc> &mv_col_descs);
private:
  int32_t rowkey_cnt_;
  int32_t col_cnt_;
  ObStoreCmpFuncs cmp_funcs_;
  common::ObHashFuncs hash_funcs_;
  common::ObHashFunc ext_hash_func_;
  ObIAllocator *allocator_;
  bool is_oracle_mode_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageDatumUtils);
};


OB_INLINE int ObStorageDatum::deep_copy(const ObStorageDatum &src, common::ObIAllocator &allocator)
{
  int ret = common::OB_SUCCESS;

  reuse();
  pack_ = src.pack_;
  if (is_null()) {
  } else if (src.len_ == 0) {
  } else if (src.is_local_buf()) {
    OB_ASSERT(src.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
    MEMCPY(buf_, src.ptr_, src.len_);
    ptr_ = buf_;
  } else {
    char * buf = static_cast<char *>(allocator.alloc(src.len_));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "allocate memory failed", K(ret), K(src));
    } else {
      MEMCPY(buf, src.ptr_, src.len_);
      // need set ptr_ after memory copy, if this == &src
      ptr_ = buf;
    }
  }
  return ret;
}

OB_INLINE int ObStorageDatum::deep_copy(const ObStorageDatum &src, char * buf, const int64_t buf_len, int64_t &pos)
{
  int ret = common::OB_SUCCESS;

  reuse();
  pack_ = src.pack_;
  if (is_null()) {
  } else if (src.len_ == 0) {
  } else if (src.is_local_buf()) {
    OB_ASSERT(src.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
    MEMCPY(buf_, src.ptr_, src.len_);
    ptr_ = buf_;
  } else if (OB_UNLIKELY(nullptr == buf || buf_len < pos + src.len_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to deep copy datum", K(ret), K(src), KP(buf), K(buf_len), K(pos));
  } else {
    MEMCPY(buf + pos, src.ptr_, src.len_);
    // need set ptr_ after memory copy, if this == &src
    ptr_ = buf + pos;
    pos += src.len_;
  }

  return ret;
}

OB_INLINE int64_t ObStorageDatum::get_deep_copy_size() const
{
  int64_t deep_copy_len = 0;
  if (is_null()) {
  } else if (is_local_buf()) {
    OB_ASSERT(len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
  } else {
    deep_copy_len = len_;
  }
  return deep_copy_len;
}

OB_INLINE int ObStorageDatum::from_buf_enhance(const char *buf, const int64_t buf_len)
{
  int ret = common::OB_SUCCESS;

  if (OB_UNLIKELY(nullptr == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to transfer from buf", K(ret), KP(buf), K(buf_len));
  } else {
    reuse();
    len_ = buf_len;
    if (buf_len > 0) {
      ptr_ = buf;
    }
  }


  return ret;
}

OB_INLINE int ObStorageDatum::from_obj_enhance(const common::ObObj &obj)
{
  int ret = common::OB_SUCCESS;

  reuse();
  if (obj.has_lob_header()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "should not have lob header", K(ret), K(obj));
  } else if (obj.is_ext()) {
    set_ext_value(obj.get_ext());
  } else if (OB_FAIL(from_obj(obj))) {
    STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(obj));
  }
  STORAGE_LOG(DEBUG, "chaser debug from obj", K(obj), K(*this));

  return ret;
}

// liangman
OB_INLINE int ObStorageDatum::my_from_obj_enhance(const common::ObObj &obj)
{
  int ret = common::OB_SUCCESS;

  reuse();

  my_from_obj(obj);

  return ret;
}

OB_INLINE int ObStorageDatum::to_obj_enhance(common::ObObj &obj, const common::ObObjMeta &meta) const
{
  int ret = common::OB_SUCCESS;
  if (has_lob_header()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "should not have lob header", K(ret), K(*this));
  } else if (is_outrow()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "lob should not set outrow in datum", K(ret), K(*this), K(obj), K(meta));
  } else if (is_ext()) {
    obj.set_ext(get_ext());
  } else if (OB_FAIL(to_obj(obj, meta))) {
    STORAGE_LOG(WARN, "Failed to transfer datum to obj", K(ret), K(*this), K(obj), K(meta));
  }

  return ret;
}

OB_INLINE ObStorageDatum& ObStorageDatum::operator=(const ObStorageDatum &other)
{
  if (&other != this) {
    reuse();
    pack_ = other.pack_;
    if (is_null()) {
    } else if (len_ == 0) {
    } else if (other.is_local_buf()) {
      OB_ASSERT(other.len_ <= common::OBJ_DATUM_NUMBER_RES_SIZE);
      MEMCPY(buf_, other.ptr_, other.len_);
      ptr_ = buf_;
    } else {
      ptr_ = other.ptr_;
    }
  }
  return *this;
}

OB_INLINE bool ObStorageDatum::operator==(const ObStorageDatum &other) const
{
  bool bret = true;
  if (is_null()) {
    bret = other.is_null();
  } else if (is_ext()) {
    bret = other.is_ext() && extend_obj_->get_ext() == other.extend_obj_->get_ext();
  } else {
    bret = ObDatum::binary_equal(*this, other);
  }
  if (!bret) {
    STORAGE_LOG(WARN, "obj and datum no equal", K(other), K(*this));
  }
  return bret;

}

OB_INLINE bool ObStorageDatum::operator==(const common::ObObj &other) const
{

  int ret = OB_SUCCESS;
  bool bret = true;
  ObStorageDatum datum;
  if (OB_FAIL(datum.from_obj_enhance(other))) {
    STORAGE_LOG(WARN, "Failed to transfer obj to datum", K(ret), K(other), K(datum));
  } else {
    bret = *this == datum;
  }
  if (!bret) {
    STORAGE_LOG(WARN, "obj and datum no equal", K(other), K(datum), KPC(this));
  }
  return bret;
}

OB_INLINE int32_t ObDatumRow::get_delta() const
{
  int32_t delta = 0;
  if (row_flag_.is_extra_delete()) {
    delta = -1;
  } else if (row_flag_.is_insert()) {
    delta = 1;
  }
  return delta;
}
OB_INLINE int64_t ObStorageDatum::storage_to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  if (is_ext()) {
    if (is_nop()) {
      J_NOP();
    } else if (is_max()) {
      BUF_PRINTF("MAX_OBJ");
    } else if (is_min()) {
      BUF_PRINTF("MIN_OBJ");
    }
  } else {
    pos = to_string(buf, buf_len);
  }

  return pos;
}

struct ObGhostRowUtil {
public:
  ObGhostRowUtil() = delete;
  ~ObGhostRowUtil() = delete;
  static int make_ghost_row(
      const int64_t sql_sequence_col_idx,
      const common::ObQueryFlag &query_flag,
      blocksstable::ObDatumRow &row);
  static int is_ghost_row(const blocksstable::ObMultiVersionRowFlag &flag, bool &is_ghost_row);
  static const int64_t GHOST_NUM = INT64_MAX;
};

} // namespace blocksstable
} // namespace oceanbase
#endif
