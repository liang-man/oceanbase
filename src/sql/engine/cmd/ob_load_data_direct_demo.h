#pragma once

#include "lib/file/ob_file.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace sql
{

class ObLoadDatumRow;
class ObLoadDataDirectDemo;

// liangman 自旋锁
class SpinlLock 
{
  std::atomic_flag flg;
public:
  void lock() {
      while(flg.test_and_set(std::memory_order_acquire));
  }
  void unlock() {
      flg.clear(std::memory_order_release);
  }
};

// liangman
pthread_mutex_t mtx_append[16];    // 在load_data()内初始化

// liangman
struct Offset {
  int64_t begin;
  int64_t end;
};

// 从文件里读到数据，存储在buffer里面
class ObLoadDataBuffer
{
public:
  friend void thread_load_csv(ObLoadDataDirectDemo *this_, sem_t *semLock, int &ret, int i);
  ObLoadDataBuffer();
  ~ObLoadDataBuffer();
  void reuse();
  void reset();
  int create(int64_t capacity);
  int squash();
  OB_INLINE char *data() const { return data_; }
  OB_INLINE char *begin() const { return data_ + begin_pos_; }
  OB_INLINE char *end() const { return data_ + end_pos_; }
  OB_INLINE bool empty() const { return end_pos_ == begin_pos_; }
  OB_INLINE int64_t get_data_size() const { return end_pos_ - begin_pos_; }
  OB_INLINE int64_t get_remain_size() const { return capacity_ - end_pos_; }
  OB_INLINE void consume(int64_t size) { begin_pos_ += size; }
  OB_INLINE void produce(int64_t size) { end_pos_ += size; }
  // liangman
  OB_INLINE void set_begin(int64_t begin) { begin_pos_ = begin; }
  OB_INLINE void set_end(int64_t end) { end_pos_ = end; }
  OB_INLINE void set_data(char *data) { data_ = data; }
  OB_INLINE void set_threadID(int id) { thread_ID_ = id; }
  OB_INLINE int threadID() const { return thread_ID_; }
  OB_INLINE int64_t begin_pos() const { return begin_pos_; }
  OB_INLINE int64_t end_pos() const { return end_pos_; }
  OB_INLINE bool is_used() const { return is_used_; }
  OB_INLINE void set_used(bool flag) { is_used_ = flag; }
  // OB_INLINE int get_surplus() const { return surplus_; }
  // OB_INLINE void set_surplus(int val) { surplus_ = val; }
private:
  common::ObArenaAllocator allocator_;
  char *data_;
  int64_t begin_pos_;
  int64_t end_pos_;
  int64_t capacity_;
  int thread_ID_ = -1;        // liangman
  bool is_used_ = false;      // liangman   默认为false，表示没有被使用  注：被使用是指buffer里存储了数据，而不是在任务队列被线程调用
  // 这个变量得作为全局变量，让所有buffer都能访问到
  // int surplus_ = 0;           // liangmna   记录固定读取2M的buffer中，多出了的部分字节，构不成一个完整的行
};

// 读本地文件
class ObLoadSequentialFileReader
{
public:
  ObLoadSequentialFileReader();
  ~ObLoadSequentialFileReader();
  int open(const ObString &filepath);
  int read_next_buffer(ObLoadDataBuffer &buffer, Offset *offset, int64_t &section_offset);
  int get_file_fd(){ return file_reader_.get_file_fd();}
private:
  common::ObFileReader file_reader_;
  int64_t offset_;
  bool is_read_end_;
};

// 把buffer里的数据按行去解析
class ObLoadCSVPaser
{
public:
  ObLoadCSVPaser();
  ~ObLoadCSVPaser();
  void reset();
  int init(const ObDataInFileStruct &format, int64_t column_count,
           common::ObCollationType collation_type);
  int get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row);
private:
  struct UnusedRowHandler
  {
    int operator()(common::ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line)
    {
      UNUSED(fields_per_line);
      return OB_SUCCESS;
    }
  };
private:
  common::ObArenaAllocator allocator_;
  common::ObCollationType collation_type_;
  ObCSVGeneralParser csv_parser_;
  common::ObNewRow row_;
  UnusedRowHandler unused_row_handler_;
  common::ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records_;
  bool is_inited_;
};

class ObLoadDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObLoadDatumRow();
  ~ObLoadDatumRow();
  void reset();
  int init(int64_t capacity);
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len, int64_t &pos);
  OB_INLINE bool is_valid() const { return count_ > 0 && nullptr != datums_; }
  DECLARE_TO_STRING;
public:
  common::ObArenaAllocator allocator_;
  int64_t capacity_;
  int64_t count_;
  blocksstable::ObStorageDatum *datums_;
};

class ObLoadDatumRowCompare
{
public:
  ObLoadDatumRowCompare();
  ~ObLoadDatumRowCompare();
  int init(int64_t rowkey_column_num, const blocksstable::ObStorageDatumUtils *datum_utils);
  bool operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs);
  int get_error_code() const { return result_code_; }
public:
  int result_code_;
private:
  int64_t rowkey_column_num_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  blocksstable::ObDatumRowkey lhs_rowkey_;
  blocksstable::ObDatumRowkey rhs_rowkey_;
  bool is_inited_;
};

// 字符集转换
// 类型转换
// 将主键字段移动到记录的头部(例如：c1 c2 c3 c4是建表是字段顺序，其中primary key为c3 c1，,所以存储时要存储为c3 c1 c2 c4)
class ObLoadRowCaster
{
public:
  ObLoadRowCaster();
  ~ObLoadRowCaster();
  // 初始化时就会建立好映射关系
  int init(const share::schema::ObTableSchema *table_schema,
           const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
  int get_casted_row(const common::ObNewRow &new_row, const ObLoadDatumRow *&datum_row);    // new_row是解析出的每个字段的值，都是string；datum_row是类型转换后的值. 字段顺序都是已经转换好了
private:
  // 做了一个映射，将存储层的字段的位置对应到数据源的字段位置
  int init_column_schemas_and_idxs(
    const share::schema::ObTableSchema *table_schema,
    const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
  // 字符集的转换和类型的转换
  int cast_obj_to_datum(const share::schema::ObColumnSchemaV2 *column_schema,
                        const common::ObObj &obj, blocksstable::ObStorageDatum &datum);
private:
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;   // 每一列的结构信息，对应的是存储的列  
  common::ObArray<int64_t> column_idxs_; // Mapping of store columns to source data columns
  int64_t column_count_;
  common::ObCollationType collation_type_;
  ObLoadDatumRow datum_row_;
  common::ObArenaAllocator cast_allocator_;
  common::ObTimeZoneInfo tz_info_;
  bool is_inited_;
};

class ObLoadExternalSort
{
public:
  ObLoadExternalSort();
  ~ObLoadExternalSort();
  int init(const share::schema::ObTableSchema *table_schema, int64_t mem_size,
           int64_t file_buf_size);
  int append_row(const ObLoadDatumRow &datum_row);
  int close();
  int get_next_row(const ObLoadDatumRow *&datum_row);
private:
  common::ObArenaAllocator allocator_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObLoadDatumRowCompare compare_;   // 排序的比较器
  storage::ObExternalSort<ObLoadDatumRow, ObLoadDatumRowCompare> external_sort_;
  bool is_closed_;
  bool is_inited_;
};

class ObLoadSSTableWriter
{
public:
  ObLoadSSTableWriter();
  ~ObLoadSSTableWriter();
  // int init(const share::schema::ObTableSchema *table_schema, blocksstable::ObMacroBlockWriter *macro_block_writers_[]);   // 初始化时用这个初始化就行
  // int append_row(const ObLoadDatumRow &datum_row, blocksstable::ObMacroBlockWriter *macro_block_writers_[], int id);    // 往macro_block_writer里写数据时，调用这个函数就行
  // int close(blocksstable::ObMacroBlockWriter *macro_block_writers_[], int index);
  int init(const share::schema::ObTableSchema *table_schema);   // 初始化时用这个初始化就行
  int append_row(const ObLoadDatumRow &datum_row, blocksstable::ObMacroBlockWriter *macro_block_writer);    // 往macro_block_writer里写数据时，调用这个函数就行
  int close(blocksstable::ObMacroBlockWriter *macro_block_writer);
  void set_close_flag(bool flag) { is_closed_ = flag; }
  blocksstable::ObDataStoreDesc data_store_desc_;
  int create_sstable();
  int64_t column_count() { return column_count_; }
  int64_t rowkey_column_num() { return rowkey_column_num_; }
  int64_t extra_rowkey_column_num() { return extra_rowkey_column_num_; }
private:
  int init_sstable_index_builder(const share::schema::ObTableSchema *table_schema);   // 构造一个sstable_index_build，用于记录每个sstable的索引
  // int init_macro_block_writer(const share::schema::ObTableSchema *table_schema, blocksstable::ObMacroBlockWriter *macro_block_writers_[]);   // 构造一个macro_block_writer，后面就是一直调用append_row()往里面塞数据。因为是单线程，所以只创建了一个writer，多线程可以创建多个
  int init_macro_block_writer(const share::schema::ObTableSchema *table_schema);
  // int create_sstable();    // 把它变为public
private:
  common::ObTabletID tablet_id_;
  storage::ObTabletHandle tablet_handle_;
  share::ObLSID ls_id_;
  storage::ObLSHandle ls_handle_;
  int64_t rowkey_column_num_;
  int64_t extra_rowkey_column_num_;
  int64_t column_count_;
  storage::ObITable::TableKey table_key_;
  blocksstable::ObSSTableIndexBuilder sstable_index_builder_;
  // blocksstable::ObDataStoreDesc data_store_desc_;    // 把它变为public
  // blocksstable::ObMacroBlockWriter macro_block_writer_;
  // blocksstable::ObMacroBlockWriter macro_block_writers_[16];
  blocksstable::ObDatumRow datum_row_;
  bool is_closed_;
  bool is_inited_;
};

class ObLoadDataDirectDemo : public ObLoadDataBase
{
  static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 2.7;
  // static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 2.6;
  // static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 4;     // 1.5G 2min36s
  // static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 2.5;      // 1.5G 2min26s    
  // static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 3;     // 1.5G 2min29s, 比500M时快了20s
  // static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 1.5;   // 提测爆内存
  // static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 1.4;   // 提测爆内存
  // static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 1.3;   // 提测爆内存
  // static const int64_t MEM_BUFFER_SIZE = (1LL << 30) / 1.2;   // 提测爆内存  //1.csv 2.2s 相比500M时，反而慢了1s，为什么？
  static const int64_t FILE_BUFFER_SIZE = (2LL << 20); // 2M
  // static const int64_t FILE_BUFFER_SIZE = (1LL << 20); // 1M
public:
  friend void thread_read_buffer(void *arg);
  friend void thread_sstable_writer(void *arg);
  friend void thread_sstable_close(void *arg);
  ObLoadDataDirectDemo();
  virtual ~ObLoadDataDirectDemo();
  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;
private:
  int inner_init(ObLoadDataStmt &load_stmt);
  int do_load(ObExecContext &ctx, ObLoadDataStmt &load_stmt);
private:
  ObLoadCSVPaser csv_parser_;
  ObLoadSequentialFileReader file_reader_;
  ObLoadDataBuffer buffer_;
  ObLoadRowCaster row_caster_;
  ObLoadExternalSort external_sort_;
  ObLoadSSTableWriter sstable_writer_;
  // ObLoadDataBuffer buffers_[8];
  // ObLoadDataBuffer buffer_0, buffer_1, buffer_2, buffer_3, buffer_4, buffer_5, buffer_6, buffer_7;
};

void thread_read_buffer(void *arg);
void thread_external_close(void *arg);
void thread_sstable_writer(void *arg);
void thread_sstable_close(void *arg);

class MyThreadPool;

class Task {
public:
  // void (*task_call_back)(void *, ObLoadCSVPaser *, ObLoadRowCaster *, ObLoadExternalSort *);
  void (*task_call_back)(void *);
  ObLoadDataDirectDemo *_this_; 
  ObLoadDataBuffer *buffer_;
  ObLoadCSVPaser *csv_parser_; 
  ObLoadRowCaster *row_caster_;
  ObLoadExternalSort *external_sorts_;
  ObLoadExternalSort *external_sort_;
  ObLoadSSTableWriter *sstable_writer_;
  Offset *offset_;
  int index_;

  void setFunc(void (*tcb)(void *)) { task_call_back = tcb; }
};

class WorkThread {
public:
  pthread_t tid_;
  int pid_;   // 从0开始，0~6
  bool usable_ = true;
  MyThreadPool *pool_;
  static void *start(void *arg);
};

class MyThreadPool : public ObThreadPool
{
public:
  MyThreadPool(int thread_count);
  // virtual ~MyThreadPool();   // 笔记：不能自己定义析构函数，会覆盖父类的析构函数，得执行父类的析构函数  
  /*
  如何发现？通过调试，发现在load_data()结束时，cpu没有利用率，函数也没有退出，单步调试发现进入了我自己定义的thread_pool的析构函数内，线程阻塞了。
  由此我就想到，我先前已经调用过mydestroy()了，这里就不需要在析构函数内继续释放，所以注释了析构函数内的内容，使得析构函数为空。
  但结果发现还是不对，现象貌似是线程仍在阻塞休眠
  由此我瞬间就想到，我是继承了ObThreadPool，那应该ObThreadPool类也需要相关的线程释放
  所以我就注释了我自己的析构函数，让去调用ObThreadPool的析构函数
  由此问题解决
  */
  void createPool();
  void push_task(void(* tcb)(void *), ObLoadDataDirectDemo *this_, ObLoadDataBuffer *buffer, ObLoadCSVPaser *csv_parser,  ObLoadRowCaster *row_caster, ObLoadExternalSort external_sorts[], Offset *offset);
  void push_task(void(* tcb)(void *), ObLoadExternalSort *external_sort);
  void push_task(void(* tcb)(void *), ObLoadExternalSort *external_sort, ObLoadSSTableWriter *sstable_writer, int i);
  int init(ObLoadDataStmt &load_stmt);
  void run1() override;
  void mydestroy();

  std::deque<Task *> task_queue_;                // 任务队列
  std::deque<WorkThread *> work_thread_queue_;   // 执行线程队列 
  int thread_count_;
  int count_;
  int task_num_;        // 任务队列中任务个数上限
  bool master_sleep_;   // 标志主线程状态，true表示睡眠，false表示活跃，初始化为false
  bool usable_ = true;
  pthread_cond_t cont_, cont_master_, cont_complete_;
  pthread_mutex_t mutex_, mutex_master_, mutex_complete_;
};

} // namespace sql
} // namespace oceanbase