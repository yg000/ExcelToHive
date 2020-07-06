package cn.sks.util

import java.util.regex.{Matcher, Pattern}


object DefineUDF {

  // 判断是否包含中文
  def isContainChinese(str:String): Boolean ={
    val pattern: Pattern = Pattern.compile("[\u4e00-\u9fa5]")
    val m: Matcher = pattern.matcher(str)
    if (m.find()) {
     return true
    }
    return false
  }
  // 判断是否包含英文
  def isContainEnglish(str:String): Boolean ={
    val pattern: Pattern = Pattern.compile("[a-zA-z]+")
    val m: Matcher = pattern.matcher(str)
    if (m.find()) {
      return true
    }
    return false
  }
  def isAllEnglish(str:String): Boolean ={
    str.matches("[a-zA-z]+")
  }
  def isAllChiness(str:String): Boolean ={
    str.matches("[\u4e00-\u9fa5]")
  }


  // 清洗 成果中 title 以及authors 中的 标签符号
  def clean_div(data:String): String ={
      if(data== null && data!="") null
      else {
          var htmlStr = data.replaceAll("&lt;","<")
        .replaceAll("&gt;",">")
        .replaceAll("&quot;","\"")
        .replaceAll("&amp;","&")
        .replaceAll("&nbsp;","")

          val script = "<script[^>]*?>[\\s\\S]*?<\\/script>" //定义script的正则表达式
          val style = "<style[^>]*?>[\\s\\S]*?<\\/style>" //定义style的正则表达式
          val html = "<[^>]+>" //定义HTML标签的正则表达式
          val p_script = Pattern.compile(script, Pattern.CASE_INSENSITIVE)
          val m_script = p_script.matcher(htmlStr)
          htmlStr = m_script.replaceAll("") //过滤script标签

          val p_style = Pattern.compile(style, Pattern.CASE_INSENSITIVE)
          val m_style = p_style.matcher(htmlStr)
          htmlStr = m_style.replaceAll("") //过滤style标签

          val p_html = Pattern.compile(html, Pattern.CASE_INSENSITIVE)
          val m_html = p_html.matcher(htmlStr)
          htmlStr = m_html.replaceAll("") //过滤html标签
            .replaceAll("#", "")
            .replaceAll("\\\\", "")
            .replaceAll("\\*", "") .replaceAll("\t", " ")
            .trim
          htmlStr
    }
  }



  //  清洗掉所有的特殊字符（融合时使用） 论文名字 单位
  def clean_fusion(data:String):String = {
    if (data == null) null
    else data.replaceAll("[^a-zA-Z0-9\u4E00-\u9FFF]","")
      .trim.toLowerCase()

  }
}


