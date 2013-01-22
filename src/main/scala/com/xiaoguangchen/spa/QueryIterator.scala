package com.xiaoguangchen.spa

/**
  * author: Chester Chen (chesterxgchen@yahoo,com)
  * Date: 1/17/13 9:28 PM
  */


trait QueryIterator[A] {

  /**
   * get next result from the result set, if there no more, the value return None.
   *
   * <pre>
   *   QueryIterator it = ...
   *   val value = it.next()
   *   while (value != None) {
   *    val r = value.get
   *    ....
   *   }
   * </pre>
   *
   * @return Some[A] if results fund, otherwise None
   */
  def next(): Option[A]
}
