# bustub 小型关系型数据库内核

## Proj2 B+树索引
- 注意：实现的索引中，叶子结点会指向对应记录的磁盘地址。这与 MySQL InnoDB 引擎是有所不同的。在 MySQL InnoDB 的索引中，主键(ID)索引的B+树叶子结点直接挂着记录的具体数据，这也被成为“B+树文件组织”结构。

**难点1**
- B+ 树本身逻辑复杂，既要考虑叶子结点，又考虑树结点，结点的分裂、合并、根的转换等十分复杂。但是，不必严格按照教材算法实现，多看几个动态插入、删除的图解，然后凭感觉和逻辑就可以把B+树的增删改查写出来。
- [待填充：描述B+TREE的插入]
- [待填充：描述B+TREE的删除]

**难点2**
- 实现线程安全的、支持并发的B+树。此时，你需要用到 PageGuard 自动释放读锁，自动取消钉住页面。加锁算法：① 对于查找操作，子结点上锁之后，可立即释放双亲结点的锁；② 插入操作，子结点上锁且未满，(从而分裂不会影响到它的祖先)，释放所有祖先结点。③ 删除操作：对于某个内部结点 > 半满才可(注意是大于，不能等于)认为其祖先结点是安全的；对于某个叶子结点，也是 > 半满，则其祖先安全；此外，无论叶子、非叶子，对于根结点的要求低一些，满足至少有一个key(对内部结点而言，这意味着有两个孩子指针)即可。
- 虽然乐观锁也可以实现且性能更好，但其实现显然在本人能力范围之外。
- 非常值得注意的是，有了 page_guard 虽然能够保护B+树的页，但是它无法保护那个十分**的数据：root_page_id_。在更新 root 的时候，必须要保证 B+ TREE root_page_id_ 和 新产生或新成为 ROOT 的页的 page_id 是同步变化的。并且在此期间，不可访问 root_page_id_，此外，也有可能读取到旧的 page_id，所以可能需要尝试使用乐观锁。

**难点3**
- 控制Evict策略，及时取消钉住页面，防止缓存池爆满带来的下面的各种BUG。
- 这通过 PageGurad 和加锁统一管理即可。自动释放资源比手动释放友好很多，这就是 C++ RAII ，即通过一个对象的生命周期管理堆内存的资源。

**Bug1 数组越界带来的未知行为：[此处是数组越界导致其后的字段page_id_部分覆盖，从而 page 无法释放]**
- 由于数组越界导致 page_id 发生了改变。而数组越界是由于叶子的深度没有上限控制，为什么叶子的上限会超过3？
- 因为在原来的实现中，叶子结点分裂分两步，先插入新结点，使得叶子结点溢出；然后再一分为二。如果叶子结点插入溢出之后，发现缓冲池是满的，无法
分裂，函数就直接返回了，叶子结点会持续处于溢出状态，并且溢出之后 IsFull 会返回 false(IsFull 采用等号判断)，使得阶为3的叶子结点也能插入245个(key, value)键值对。如果 buffer_pool 较小，叶子结点最终会溢出而无法分裂，溢出会导致数组越界，Page 的 page_id_ 会被部分覆盖写入，从而导致Pageguard在释放这个页时无法匹配原来的 page_id_[可扩展哈希表查找失败]，释放失败，这个页就会长期驻留在缓冲池中。

- 为什么buffer_pool较小而数据规模scale较大时，会直接卡死？比如 size = 30 的 buffer_pool，B+ 树的 n(阶)保持最大(245)，插入 1000 个数据会在 9452 左右卡死？
- 直接卡死是因为缓冲池满了，没有可驱逐的页面，无法从磁盘中读入新的页面了，程序无法向下运行。
- size = 30 的缓冲池是够用的，并不是不够，因为B+树，特别是240+阶的B+树，深度不会超过5，递归调用不会过多。之所以缓冲池会逐步变满导致卡死，是因为：245深度的叶子结点，如果采用“先插入(从而导致页溢出，会占用其它未知内存)，再分裂”的方式，会导致这个短时间溢出的叶子结点的page_id_发生变化(内存溢出，覆盖写入page_id_)，从而Pageguard在释放这个页时无法匹配原来的 page_id_，pin_count_ 无法减到0，这个页会长期驻留在缓冲池中。这会导致缓冲池的空间越来越小，最终填满整个缓冲池。

- 缓冲池相对于插入的结点数量而言过小会导致哪些问题？
- 因为buffer_pool较小，会导插入结点后分裂失败(前提是采用先插入[会有一段时间溢出]，后分裂)(不能获取到新的page)，导致数组越界(本来希望最多存俩key，结果存了253个key)[注意：这种情况由查找算法也不会找到插入的那些数据]，数组越界就会修改其后面 page_id_ 的数值。page_id_ 改变会导致页面释放失败，从而 buffer_pool 很快会被占满，内存无法及时得到释放，(以 key == 3 为例)即本次 Insert 之后，叶子结点大小变成 3，之后变成 4 ...(IsFull仅对等于keynum==2的情况生效)

- 解决方案：① 从根本上，应该防止数组越界，防止任何页出现短暂的“溢出”，从而防止页面无法被及时释放。② 及时 EVICT 不需要的页面，当然这使用 PageGuard 进行保护即可。② 用户输入的 size，你在构造函数里×2，“预留空间”，比如 pool_size = 2 * size，将缓冲池内部设置的比较大即可，但这只是权宜之计，不是什么好办法，因为程序本身就有问题。

**Bug2 PageGuard本身存在的Bug**
- 等号构造时要先舍弃自己的资源，然后才能接收别人的资源。
```C++
auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
    if (this == &that) {
      return *this;
    }
    //! \bug 注意：一定要先放弃当前拥有的资源！
    Drop();                         
    guard_ = std::move(that.guard_);  // 等号赋值转移所有权，这可以保证锁的状态是不变的。
    return *this;
  }
```
- 防止一个锁或者一个资源被释放两次。

**Bug3 B+树实现本身的BUG**
- 比如左右兄弟写错、交换或借入的结点不对等各类问题。但幸好该实验提供了B+树可视化工具，方便调试。

**项目优化的一些要点**
- 从 LaderBoard 的排名来看，二分查找带来的效率提升功不可没。在实现时，所有查找操作，包括查找插入位置，皆使用二分查找。
- 此外，root_page_id_ 和 root_page 的变换是难以维护的，我也写不对。所以应该采用一个比较明智的策略，正如我们在leetcode上处理链表头结点时，往往添加一个哑结点，B+树为了更好地处理Root，可以使用一个额外的page来作为根结点的祖先，这个page只负责指向真正的根结点。然后成员变量 root_page_id_ 指向这个额外的页即可。每次直到读到根结点，并发现根结点是安全的，就释放额外的页上面的锁。比如插入操作时，根结点未满，则根是安全的；删除操作时，根结点的孩子指针个数 > 2(注意：等于2则根结点可能被合并，即此时根是不安全的) 就认为根结点是安全的，即可释放额外页。对于查找操作，读入根结点的页之后即可立即释放额外的页。如果一直持有额外的页，就相当于加大锁，[当然，还是有区别的，比如缓冲池的页还是可以及时被刷出到磁盘的]，没有什么并发性。
- 加锁原则：先安全，后高并发。可以先加粒度比较大的锁，就像Java HashTable 那样，所有对外的方法都加大锁，先保证线程安全。然后逐步优化为细粒度锁。不要想着一步到位。

**一些八股补充？**
- 为什么 MySQL 采用 B+ TREE 索引的时候要将元组本身挂在叶子结点上？
- 一个思考：在插入或者删除元组时更容易维护。因为你只需要对叶子结点对应的这个page(16KB)进行修改。如果你在叶子结点存放磁盘地址，这在查找时没什么问题，只不过会多一次 I/O；在插入元组时，如果你每次插入在数据表文件的末尾，那么索引的先后顺序将不再代表磁盘中元组的先后顺序，如果使用B+树进行范围查询，取出一系列记录磁盘地址后，可能带来较多的随机磁盘 I/O，而随机磁盘 I/O 的效率低于 顺序磁盘IO。当然，这些是机械盘的情况，如果使用 SSD 固态盘，情况可能会有所不同。如果每次插入元组，插入在数据表文件的以主码为顺序的合适位置，(主索引始终保持聚簇)，那么每次也需要维护磁盘文件中记录的顺序以保证元组的有序性，这可能需要在数据表文件中移动较多的记录。

## Proj3 算子执行器
**准备：代码是如何组织的？**
- 假设我们执行 SELECT colA FROM __mock_table_1; 这个简单的查询。
- 核心函数：直接锁定 execution_engine.h 里面的 Execute 函数。
- const AbstractPlanNodeRef &plan 是经过了 Parser -> Binder -> Planner -> Optimizer 四个阶段产生的东西。这个 plan 打印出来的效果就是 
```
Projection { exprs=[#0.0] } | (__mock_table_1.colA:INTEGER)
  MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
```
- Execute 函数中包含了 auto executor = ExecutorFactory::CreateExecutor(exec_ctx, plan); 它会递归地创建算子执行器，使得它形成一颗树。在构造
子结点的时候，plan 也会做相应调整，使得子节点不可以看到父节点的 plan。plan = insert_plan->GetChildPlan()，比如以上面的 plan (子结点是 MockScan ，父结点(根结点)是Projection) 为例，两个结点的 plan_ 如下所示：
```
MockScan Executor 的 plan_ 变量是 [只有下面一行]
MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)

Projection Executor 的 plan_ 变量是 [同时包含下面两行]
Projection { exprs=[#0.0] } | (__mock_table_1.colA:INTEGER)
  MockScan { table=__mock_table_1 } | (__mock_table_1.colA:INTEGER, __mock_table_1.colB:INTEGER)
```
- 提示：一个算子只能看到它自己及算子树中它的子结点的 plan，并且自己的 plan 永远处于最上层。
- executor->Init(); 的调用也是递归的，相当于对整个算子树的所有 executor 都进行了初始化，递归出口或许是 MockScan Executor。MockScan Executor[全表扫描算子] 的初始化是 cursor_ = 0;
- 每个算子的核心函数是 Next()，GetOutputSchema().GetColumnCount() 可以统计这个算子执行后输出的列数。因此，由于有 plan_ 变量，所以算子是知道自己输出的关系模式的。
- expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()) 通过输入子结点的计算结果计算父结点的表达式。如果执行的是  exprs=[#0.0] 或者 exprs = [(2 + 3), (4 + 5)] 则这个 expr->Evaluate 似乎会执行两次...

- values (1, 2, 'a'), (3, 4, 'b'); 这种可以直接创建一个两行，三列的表。它的执行计划如下
```
Values { rows=2 } | (__values#0.0:INTEGER, __values#0.1:INTEGER, __values#0.2:VARCHAR)
```
**顺序扫描，IndexScan的理解**
- 为什么 order by 会用到 index scan ？ 
- 因为可以这样执行：先在待排序 key 对应的 B+ 树索引中顺序扫描，按照顺序取出所有 key，随后去数据库表文件中逐个读取即可。由此就恰好按照完成
了一次排序。[并且是按照 key 的顺序]
- B+ 树的叶子结点的 value 表示其磁盘地址，或许正是我们要找的 RID，只要你有 rid，找到数据表文件的开头的磁盘地址[一般都是按照字节寻址]，经过一些偏移量的计算即可找到对应的元组。[key --> rid].
- 所以, IndexScan 的实现思路是: 先顺序扫描索引,找到一个 RID 序列(按key有序),然后再去数据表文件(table_heap_)中按照 RID 逐个把元素查找出来即可.由于表文件是按照 RID 有序的, 所以这样查询文件效率更高,并且你查到的所有元组出现的先后次序都是有序的. 如果你不经过索引,你就不得不对文件进行外部排序,或者把所有元组读入然后内排,这是不可接受的. 
- 索引扫描如下所示,所以你可以通过 index_oid (即索引的唯一标识 id) 直接定位到某个 key 索引的 B+ TREE.
```
IndexScan { index_oid=0 } | (t2.v3:INTEGER, t2.v4:INTEGER)
```
**算法：聚合函数(与去重函数)的哈希算法**
- 下面我们以计算 select student_name, avg(score) from student_score group by student_name;(计算每个学生的平均成绩)
- ① 分区：由于数据表很大，我们难以得到完全在内存中的哈希表。如果有 where 子句，那么你需要先过滤，然后再投影到仅剩 
student_name 和 score 这两列。然后，使用哈希函数，将表的记录放到 B 个桶中。这里只是一个形式上的哈希，本质上不是 HASH，
只是在分区，所以，哈希函数一般就是 f(key) = key % B 这种。注意：这个阶段的每个分区或许都需要持久化到磁盘上。
- ② rehash(再哈希)：对于上面的每个分区(提示：一个分区可能对应多个磁盘页)，你需要[再用一个其它的哈希函数]，创建一个内存中的
哈希表，完成结果汇总：[1]如果你找到了对应的分区key，那么你只需要更新汇总字段，这里就是平均成绩；[2]如果没找到，就需要向哈希表
新添加一个 [key -- 汇总字段] 这样的表项。这里，哈希冲突问题可以采用线性探测再散列的做法。注意：在聚合函数中，你的哈希表是<K, V>键值对的形式。
- ③ 每次一个分区处理完成后，你需要把这个内存中的哈希表清空，以供下一个分区使用。
- 提示：在 proj3，你无需处理 ① 分区这个步骤，你应该是只需要处理 rehash 的过程。

**聚合函数在本项目中的实现**
- 在初始化阶段将某个元组分为 group by keys + agg values 两部分，然后使用 group by keys 作为 HASH KEY，
将该元组的 agg values 更新到[比如 count 就是 + 1，sum 就是把 value 也加上]对应的 hash key 中即可。

**Join的实现：索引嵌套循环连接(NestedLoopJoin)**
- 用于处理自然连接和等值连接。
- 假设是 t 连接 s，那么 s 是右侧表，我们使用的就是右侧表的索引。比如，左侧表中拿出一个元组，ID = 2038，那么你就需要通过B+树索引到右侧表中寻找
到所有 ID = 2038 的 元组，这种做法的好处在于，你无需对于每个左侧元组，都对右侧进行一次全表扫描。(注意：B+树的叶子结点上面存放的就是 key --> RID 的键值对，所谓 RID，就是你在proj2中的磁盘地址。)
- 提示：注意：在算子树中，普通嵌套循环有两个子算子结点；而索引嵌套循环仅有左表的一个算子结点。

**项目本身的一些坑**
- select * from t1 order by v1; 未必会优化为 IndexScan; 可能优化为 SeqScan + Sort....
- 当然,  SeqScan + Sort 的实现方式就非常简单粗暴了, 把所有元素直接读入堆内存, 然后 std::sort 即可

**Bug记录区**
-**① 非常好 BUG，爱来自 Proj2.** Proj2 的 root page 需要你自己分配，header page 是系统赠送的，它维护了 B+ TREE 的元信息。每次变换 root_page_id_ 的时候，你都需要执行函数 UpdateRootPageId(0) 以更新这个 HeaderPage 里面的 root_page_id_. 在你原来的实现中，直接将 header_page 本身当成了 root page，所有 tree 的 root_page_id 都是 HEADER_PAGE_ID(0)。这可能在 proj2 中不会出现问题， 但是你用相同buffer_pool_ 构建第二个树的索引时，page_id 依然从0开始，则原来第一颗树的 root page 会被覆盖，于是你获得的索引就是乱序的，甚至都无法得知访问的是哪个B+树。我都不知道proj2测试为什么能过....
- **② 神奇的错误：**
```C++
// 错误的写法
//! \note 注意：由于 DeleteEntry 第一个参数是 key，要求你传入只有一个 key 的元组 (key)，而不是整个 child 元组
index_info->index_->DeleteEntry(child_tuple, *rid, exec_ctx_->GetTransaction());

// 正确的写法
Tuple key{child_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), 
                       *(index_info->index_->GetKeySchema()), index_info->index_->GetKeyAttrs())};
index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
```
- **③ 可恶的嵌套循环连接 Join。** 错误体现在，在左连接的时候，如果生成悬浮元组，然后调用 NextAndReset ，随后函数直接返回；由于 NextAndReset 会导致右侧表的游标下移一个单位，所以会忽略右侧表的第一个元组，造成连接错误。
- **④ 索引嵌套循环连接中的错误：**
```C++
// 对于某个不能匹配的左表元组，如果是内连接，那么需要向下迭代
if (!NextAndReset()) {
  return false;
} else {
  continue; // 这里需要continue 进行下次循环！
            // 继续向下执行的话，由于 result(一个 vector) 是空，你取了 result[0] 这会直接给你 SEGMENTATION DEFAULT。
}
```
- **⑤ 构造与初始化：** 聚合函数聚合时的那个哈希表需要在 Init() 中初始化，否则你多次调用 Init()，聚合值会进行累计。
类似的问题还存在于 IndexScan 的 sorted_rids_ 数据结构，每次初始化必须清空该结构。否则元素会在里面富集。
下面的代码展示了正确的聚合函数初始化代码，并标记了 bug 所在之处。以 count 为例，如果没有 clear 那一行，将会
使得该算子每次执行 Init 的结果累计，第一次是 2， 第二次是 4，第三次是 6...
```C++
void AggregationExecutor::Init() {
    child_->Init();
    aht_.Clear(); //! \bug 每次初始化都需要清空原有汇总表，防止汇总结果每调用一次 Init 就进行一次累加
    Tuple child_tuple{};
    RID child_tuple_id{};
```

