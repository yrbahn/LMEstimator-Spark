package sparklm

trait SparkLMCommand
case object PreProcessing extends SparkLMCommand
case object CountNGrams extends SparkLMCommand
case object GenDiscount extends SparkLMCommand
case object NGramCount extends SparkLMCommand
case object CreateDictionary extends SparkLMCommand
case object AdjustedCount extends SparkLMCommand
case object UninterpolationProb extends SparkLMCommand
case object InterpolationProb extends SparkLMCommand
case object BackOff extends SparkLMCommand
case object ARPAFormat extends SparkLMCommand
//case object ALLCommand extends SparkLMCommand

