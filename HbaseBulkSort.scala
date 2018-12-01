package com.my.sort

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

/**
  * @author: Mr.Zhu
  * @create: 2018-11-28 20:13
  *
  * 写 Hfile 数据时是要求有序的，
  * 由于spark自带的排序方法无法保证写数据时的顺序，
  * 所以自己 创建样例类重写排序方法
  *
  * Hfile 的顺序要求如下
  * 一.key 有序
  * 二.value有序
  * value中的顺序为
  *     1.rowkey
  *     2.Column family
  *     3.field
  *
  * 排序顺序为
  *     1.比较key的大小
  *     2.比较vakue中rowkey的大小
  *     3.比较Cloumn family的大小
  *     4.比较field 的大小
  *
  * @param key    对 Hbase 行键 序列化的封装对象
  * @param value  对 Hbase 行键、列族、字段名、值的封装对象
  * @param return 进行比较后的值
  */

case class HbaseBulkSort(key: ImmutableBytesWritable, value: KeyValue) extends Ordered[HbaseBulkSort] {

  override def compare(that: HbaseBulkSort): Int = {
    var num = 0
    if (this.key != that.key) {
      num = this.key.compareTo(that.key)
    } else {

      /**
        * 正常来说应该按照以下顺序进行排序
        * row=rowkey  family=Column family  qualifier=field
        *
        * 因为key就是rowKey序列化后的对象
        * 但是我们已经根据key进行排序了所以在value
        * 排序中直接跳过rowkey排序来避免过多字段的排序造成效率低下
        */

      val fileds = Array("family", "qualifier")
      val map1 = this.value.toStringMap
      val map2 = that.value.toStringMap
      var i = 0
      while (i < fileds.length) {
        val v1 = map1.get(fileds(i)).toString
        val v2 = map2.get(fileds(i)).toString
        if (v1 != v2) {
          num = v1.compareTo(v2)
          i = fileds.length
        }
        i = i + 1
      }
    }
    num
  }
}
