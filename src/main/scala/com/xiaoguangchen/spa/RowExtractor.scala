package com.xiaoguangchen.spa

/**
 * Chester Chen (chesterxgchen@yahoo.com)
 * User: Chester Chen
 * Date: 12/27/12
 * Time: 10:39 AM
 *
 */


trait RowExtractor[T] {
  def extractRow (oneRow: Map[ColumnMetadata, Any]) : T
}
