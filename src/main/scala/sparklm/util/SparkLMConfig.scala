package sparklm.util

import scala.collection.JavaConversions._
import com.typesafe.config.{ConfigException, Config}

case class SparkLMConfig(config: Config){


  val inputs  = {
    val _inputs = config.getStringList("sparklm.inputs").toSeq
    if(_inputs.isEmpty){
      throw new RuntimeException("inputs is empty!")
    } else
      _inputs
  }

  val outputDir : String = {
    val output = config.getString("sparklm.output_dir")
    if (output.trim.isEmpty)
      throw new RuntimeException("output is empty")
    else
      output
  }

  val alluxioDir =
      try
        config.getString("sparklm.alluxio_dir")
      catch {
        case _ :  Throwable =>
          outputDir
      }


  val preprocessedOutputDir  = s"$alluxioDir/preprocessed"
  val ngramCountOutputDir = s"$alluxioDir/ngram_count"
  val adjustedCountOutputDir  = s"$alluxioDir/adjusted_count"
  val uninterpolatedProbOutputDir = s"$alluxioDir/uninterp"
  val interpolatedProbOutputDir = s"$alluxioDir/interp"


  val outputFormat : OutputFormat = {
    val _outputFormat = config.getString("sparklm.output_format")
    if (_outputFormat.trim.isEmpty) {
      throw new RuntimeException("output is empty")
    } else
      if (_outputFormat == "arpa")
        ARPAFormat
      else
        TextFormat
  }

  val startSymbol = getStringElse("sparklm.dictionary.start_symbol", "<s>")
  val endSymbol = getStringElse("sparklm.dictionary.end_symbol", "</s>")
  val dictWordCount = config.getInt("sparklm.dictionary.word_count")
  val dictFileName = getStringElse("sparklm.dictionary.file_name", "wordDict.txt")
  val dictReadPath = getStringOption("sparklm.dictionary.read_path")


  val dictSavePath = config.getString("sparklm.dictionary.save_path")
  val unknownWordSymbol = config.getString("sparklm.dictionary.unk")

  val discount = config.getDouble("sparklm.ngram.discount")
  val nGramOrder = config.getInt("sparklm.ngram.order")
  val maxModKNSpecialCount = getIntElse("sparklm.ngram.max_mod_kn_special_count", 3)

  val minCountsPre : List[Int] = {
    try
      config.getIntList("sparklm.ngram.min_counts_pre")
        .map(_.toInt).toList
    catch {
      case _ : ConfigException =>
        List(1,1,2,2,2,2,2,2,2,2,2)
    }
  }

  val pruneCnt = getIntElse("sparklm.ngram.prune_cnt", 0)

  val ignoreQueryCount =
    try
      config.getBoolean("sparklm.ngram.ignore_qc")
    catch {
      case _ : ConfigException => false
    }

  val tokenizer = getStringElse("sparklm.tokenizer.program", "twitter")
  val tokenizerPath = getStringElse("sparklm.tokenizer.path", "")

  val debug = getBooleanElse("sparklm.debug", false)

  val filterEnable = getBooleanElse("sparklm.filer_enable", true)

  val filterConfig = {
    val maxSentenceLen = getIntElse("sparklm.filter.sentence_max_length", 256)
    val minSentenceLen = getIntElse("sparklm.filter.sentence_min_length", 1)
    val maxWordLen = getIntElse("sparklm.filter.word_max_length", 30)
    val maxCombiWordCount = getIntElse("sparklm.filter.combi_word_max_count", 15)
    val qcMaxCount = getIntElse("sparklm.filter.qc_max_count", 0)
    FilterConfig(maxSentenceLen, minSentenceLen, maxWordLen, maxCombiWordCount, qcMaxCount)
  }

  val normalizationEnable = getBooleanElse("sparklm.normalization_enable", true)

  val whiteListPath = getStringOption("sparklm.bwlist.whitelist_path")
  val blackListPath = getStringOption("sparklm.bwlist.blacklist_path")

  val arpaFormat = getBooleanElse("sparklm.arpa", false)

  def getIntElse(field: String, default: Int) = {
    try
      config.getInt(field)
    catch {
      case _ : ConfigException =>
        default
    }
  }

  def getBooleanElse(field: String, default: Boolean) =
    try
      config.getBoolean(field)
    catch {
      case _ : ConfigException =>
        default
    }

  def getStringElse(field: String, default: String) =
    try
      config.getString(field)
    catch {
      case _: ConfigException => default
    }

  def getStringOption(field: String) : Option[String] =
    try
      Some(config.getString(field))
    catch {
      case _: ConfigException => None
    }
}

case class FilterConfig(maxSentenceLen: Int, minSentenceLen: Int, maxWordLen: Int,
                        maxCombiWordCount: Int, qcMaxCount:Int)

trait OutputFormat
case object ARPAFormat extends OutputFormat
case object TextFormat extends OutputFormat
