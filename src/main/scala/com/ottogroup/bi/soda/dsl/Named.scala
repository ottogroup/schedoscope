package com.ottogroup.bi.soda.dsl

trait Named {
  def namingBase: String

  def n = Named.formatName(namingBase)
}

object Named {
  def formatName(name: String) = {
    val formattedName = new StringBuffer()
    for (c <- name) {
      if (c >= 'A' && c <= 'Z')
        formattedName.append("_" + c.toString.toLowerCase())
      else
        formattedName.append(c)
    }
    formattedName.toString().replaceAll("^_", "")
  }
}