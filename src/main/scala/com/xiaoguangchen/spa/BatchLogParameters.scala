package com.xiaoguangchen.spa

import collection.mutable


/**
 * User: Chester_Chen
 * Date: 5/6/12
 * Time: 1:06 PM
 */


private[spa] class BatchLogParameters(val pos: mutable.Map[Int, SQLParameter], val named: mutable.Map[String, SQLParameter]);
