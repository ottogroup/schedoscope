package com.ottogroup.bi.soda.dsl.transformations

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.Text
import java.io.IOException
import org.apache.hadoop.mapreduce.Reducer

class FailingMapper extends Mapper[Text,Text,Text,Text] {

  throw new IOException("failing like hell")
  
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def map(key: Text, value: Text, context: Context) {
    //FIXME: why can't I overwrite map here??
    throw new IOException("failing like hell")
  }    
  
}