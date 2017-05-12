package sparklm

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

import sparklm.util._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.LogManager
import org.clapper.argot.ArgotParser

object SparkLM{

  @transient lazy val log = LogManager.getLogger("LMSpark")

  val ZEROTON_ID = -1
  val BOS_ID = 2
  val BOS = "<s>"
  val EOS_ID = 1
  val EOS = "</s>"
  val SIL = "sil"
  val UNK_STR1 = "<UNK>"
  val UNK_STR2 = "zz:OOV:###"
  val UNK_PRN2 = "###\ta:OV#"

  val SPACE_TAG = "<sp>"

  val LOG_PROB_OF_ZERO = -99

  case class Args(cmd: SparkLMCommand, config: Config, prndictFile: Option[String]=None)

  def main(args:Array[String]):Unit = {

    val sparkConf = new SparkConf(true).setAppName("LMSpark")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UseCompressedOops")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.registrator", "sparklm.util.MyKryoRegistrator")

    implicit val sc = new SparkContext(sparkConf)

    val argsObj = readArgs(args)

    val sparkLMConfig = SparkLMConfig(argsObj.config)

    val result : Either[String, Unit] =
      argsObj.cmd match {
        case NGramCount =>
          nGramCount(sparkLMConfig)
        case AdjustedCount =>
          adjustedCount(sparkLMConfig)
        case GenDiscount =>
          genDiscount(sparkLMConfig)
        case UninterpolationProb =>
          uninterpolationProb(sparkLMConfig)
        case InterpolationProb =>
          interpolationProb(sparkLMConfig)
        case BackOff =>
          backOff(sparkLMConfig)
        case PreProcessing =>
          preprocessing(sparkLMConfig)
        case _ =>
          Left("unknown command")
      }

    if (result.isRight){
      //result.right
      log.info("success")
    } else {
      log.error(s"${result.left}")
    }

    sc.stop()
  }

  def readArgs(args: Array[String]) = {
    val parser = new ArgotParser(
      programName = "sparklm",
      compactUsage = true,
      preUsage = Some("%s: Version %s. Copyright (c) 2015, %s.".format(
        "SparkLM",
        "0.1",
        "")
      )
    )

    val config = parser.option[Config](List("config"),
      "filename",
      "Configuration file.") {
      (c, opt) =>
        if(c.startsWith("http://")){
          try {
            val configContent = scala.io.Source.fromURL(c).mkString
            ConfigFactory.parseString(configContent).resolve
          } catch {
            case _ : Throwable =>
              parser.usage("Configuration file \"%s\" does not exist".format(c))
              ConfigFactory.empty()
          }
        } else {
          val file = new File(c)
          if (file.exists) {
            try {
              ConfigFactory.parseFile(file)
            } catch {
              case _ : Throwable =>
                parser.usage("Configuration file \"%s\" does not exist".format(c))
                ConfigFactory.empty()
            }
          } else {
            parser.usage("Configuration file \"%s\" does not exist".format(c))
            ConfigFactory.empty()
          }
        }
    }

    val command = parser.option[SparkLMCommand](List("command"),"cmd", "lm command"){
      (c, _) =>
        c match {
          case "count_ngrams" =>
            CountNGrams
          case "ngram_count" =>
            NGramCount
          case "adjusted_count" =>
            AdjustedCount
          case "gen_discount" =>
           GenDiscount
          case "uninterp_prob" =>
           UninterpolationProb
          case "interp_prob" =>
           InterpolationProb
          case "backoff" =>
           BackOff
          case "arpa" =>
           ARPAFormat
          case "preprocessing" =>
            PreProcessing
          case _ =>
            CountNGrams
        }
    }

    val prndict = parser.option[String](List("prndict"),"pdict", "prndict"){(c, _) => c}

    parser.parse(args)

    val conf = config.value.getOrElse(throw new RuntimeException("--config argument must be provided"))
    val cmd = command.value.getOrElse(throw new RuntimeException("--command argument must be provided"))
    Args(cmd, conf, prndict.value)
  }

  /**
    * Preprocessing corpus
    * @param sparkLMConfig
    * @param sc
    * @return
    */
  def preprocessing(sparkLMConfig: SparkLMConfig, prndictFile: Option[String]=None)(implicit sc: SparkContext): Either[String, Unit] = {
    val bwList = BWList(sparkLMConfig.whiteListPath, sparkLMConfig.blackListPath)
    if (bwList.isEmpty) throw new RuntimeException("cannot load bwlist")


    val rawSentenceRDD = DataSource.toRawSentenceRDD(sparkLMConfig)
    val sentenceRDD = rawSentenceRDD.preprocess(sc.broadcast(bwList.get))
    sentenceRDD.cache()

    if(prndictFile.isDefined) {
      SparkLMDict.load(sparkLMConfig, prndictFile).fold(Left(_), {wordSeq =>
        val word2Idx = SparkLMDict.word2Idx(wordSeq)
        val sentenceIdRDD = sentenceRDD.toSentenceIdRDD(sparkLMConfig.nGramOrder, sc.broadcast(word2Idx))
        sentenceIdRDD.save
      })
    } else {
      SparkLMDict.create(sparkLMConfig, sentenceRDD).fold(Left(_), { wordSeq =>
        SparkLMDict.save(sparkLMConfig, wordSeq, sparkLMConfig.outputDir)
        val word2Idx = SparkLMDict.word2Idx(wordSeq)
        val sentenceIdRDD = sentenceRDD.toSentenceIdRDD(sparkLMConfig.nGramOrder, sc.broadcast(word2Idx))
        sentenceIdRDD.save
      })
    }
  }


  /**
    * NGram Count
    * @param sparkLMConfig
    * @param sc
    * @return
    */
  def nGramCount(sparkLMConfig: SparkLMConfig) (implicit sc: SparkContext) = {
    val sentenceIdRDD = DataSource.loadSentenceIdRDD(sparkLMConfig)
    val nGramCountRDD = sentenceIdRDD.toNGramCountRDD(sparkLMConfig.nGramOrder)
    nGramCountRDD.save
  }

  /**
    * Get Adjusted Count from NGram
    * @param sparkLMConfig
    * @param sc
    * @return
    */
  def adjustedCount(sparkLMConfig: SparkLMConfig) (implicit sc: SparkContext) = {
    val sentenceIdRDD = DataSource.loadSentenceIdRDD(sparkLMConfig)
    val nGramCountRDD = sentenceIdRDD.toNGramCountRDD(sparkLMConfig.nGramOrder)
    val adjustedCountRDD = nGramCountRDD.adjustedCount(sparkLMConfig.nGramOrder)
    adjustedCountRDD.save
  }

  /**
    * Calculates Discounts
    * @param sparkLMConfig
    * @param sc
    * @return
    */
  def genDiscount(sparkLMConfig: SparkLMConfig)
                  (implicit sc: SparkContext) = {
    val adjustedCountNGramRDD = DataSource.loadAdjustedCountNGramRDD(sparkLMConfig)
    val countOfCountList = adjustedCountNGramRDD.collectCountOfCountList

    val discountMap = DiscountMap(countOfCountList, sparkLMConfig.nGramOrder, sparkLMConfig.maxModKNSpecialCount)
    discountMap.save(sparkLMConfig.outputDir)
  }

  /**
    * Calculates uninterpolated prob.
    * @param sparkLMConfig
    * @param sc
    * @return
    */
  def uninterpolationProb(sparkLMConfig: SparkLMConfig)
                         (implicit sc: SparkContext) = {
    DiscountMap.load(sparkLMConfig)
      .fold(_ => Left("cannot load discountmap"), { discountMap =>
        val adjustedCountNGramRDD = DataSource.loadAdjustedCountNGramRDD(sparkLMConfig)
        val uninterpIntermediateNGramRDD = adjustedCountNGramRDD.toUninterpolatedIntermediatedNGramRDD
        val uninterpNGramRDD =
          uninterpIntermediateNGramRDD.toUninterpolatedNGramRDD(sc.broadcast(discountMap))
        uninterpNGramRDD.save
      })}

  /**
    * Calculates interpolated Prob.
    * @param sparkLMConfig
    * @param sc
    * @return
    */
  def interpolationProb(sparkLMConfig: SparkLMConfig)
                       (implicit sc: SparkContext) = {
    val uninterpNGramRDD = DataSource.loadUninterpolatedNGramRDD(sparkLMConfig)
    val uninterpUniGrams = uninterpNGramRDD.getUniGrams
    val interpNGramRDD = uninterpNGramRDD.toInterpolationNGramRDD(sc.broadcast(uninterpUniGrams))
    interpNGramRDD.save
  }

  /**
    * Calculates BackOff
    * @param sparkLMConfig
    * @param sc
    * @return
    */
  def backOff(sparkLMConfig: SparkLMConfig)
             (implicit sc: SparkContext) = {
    SparkLMDict.load(sparkLMConfig)
      .fold(_ => Left("loading wordDict errors"), { wordSeq =>
        val uniGramCnt = sc.longAccumulator("unigram_count")
        val biGramCnt = sc.longAccumulator("bigram_count")
        val triGramCnt = sc.longAccumulator("trigram_count")
        val word2idx = SparkLMDict.word2Idx(wordSeq)
        val unkId = SparkLMDict.getUnkId(word2idx, sparkLMConfig.arpaFormat)
        val interpNGramRDD = DataSource.loadInterpolatedNGramRDD(sparkLMConfig)
        val backoffRDD = interpNGramRDD.toBackOffNGramRDD(unkId)

        val idx2word = SparkLMDict.idx2word(wordSeq)

        backoffRDD.saveTextFormat(uniGramCnt, biGramCnt, triGramCnt,
          sc.broadcast(idx2word))

      })
  }
}
