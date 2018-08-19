# FEpipeline
A easy to get start, scalable, distributed feature engineering framework based on Spark.

## FEpipeline Framework
![](https://i.loli.net/2018/08/17/5b75bc2363823.jpg)
The project refers to the architectural design of Spark ML and Keras and enable the feature engineering pipeline to abstract into modules as SubSampler, Convertor, Assembler, Stater, Discretizer, OutputFormater and Evaluator. These modules or some modules can be input into a pipeline in arbitrary arrangement according to your customized requirements, and obtained output format according to the predetermined. Meanwhile, you can evaluate and select some features depend on your requirements.

## Modules
* SubSampler: Subsample samples or raw samples as defined strategy (strategies), such as OneEqualOneSubsampler, ManyContainOneSubsampler, RandomSubsampler, SkipAboveSubsampler, UserSubsampler, NegativeSubsampler etc.
* Convertor: Convert String or Array or any other non-numeric feature to numeric feature, even do some normalized conversion. Such as TitleLenConvertor, WelCntConvertor etc.
* Assembler: Assemble some features to a feature. It can be used to make cross features manually.
* Stater: Get feature statistics attribute value by Aggregater (e.g. pv), CXRStater (e.g. ctr/cvr), BayesianSmoothing etc.
* Discretizer: Discretize feature by the same number in each field (SameNumDiscretizer), or by the evenly divided fields (EqualFieldDiscretizer) etc.
* OutputFormater: Define output format such as FtrlOutputFormater, LibsvmOutputFormater etc.
* Evaluator: It is be used to evaluate the feature degree of importance and divided to StatisticsEvaluator and InformationEvaluator. In StatisticsEvaluator, it has Coverage, Correlation, TopNFreqValue, Quantiles and Chi-square test etc. In InformationEvaluator, it has InfoGain, InfoGainRatio, InfoValue etc.

## Code Snippet

```
val spark = SparkSession
  .builder()
  .master("local[8]")
  .appName("[Huye] FEpipelineExample")
  .enableHiveSupport()
  .getOrCreate()

val outPath = args(0)
val rawSample = spark.table("raw_sample")

// Define the map between column name and feature index for FtrlOutputFormater.
val colNameIndexes = Array("age"-> 1, "score" -> 2, "title_len"-> 3, "age_score_titlelen" -> 4,
  "pv" -> 5, "infoid_ctr" -> 6, "title_len_disc" -> 7, "infoid_ctr_disc" -> 8)

// Define the feature pipeline.
val pipeline = Pipeline(Array(
  OneEqualOneSubsampler("app", "platform"),
  TitleLenConvertor("title", "title_len"),
  Assembler(Array("age", "score", "title_len"), "age_score_titlelen"),
  Aggregater("infoid", "pv"),
  CXRStater("label", "infoid", "infoid_ctr"),
  SameNumDiscretizer("title_len", "title_len_disc", 5),
  SameNumDiscretizer("infoid_ctr", "infoid_ctr_disc", 10),
  FtrlOutputFormater(colNameIndexes, "sample")))

val pipelineModel = pipeline.fit(rawSample)
val sample = pipelineModel.transform(rawSample)

sample.write.save(outPath)

// Feature evaluation
val statisticsEvaluator = StatisticsEvaluator(rawSample, "age", "coverage")
val coverage = statisticsEvaluator.evaluate
val informationEvaluator = InformationEvaluator(rawSample, "label", "age", "infoGain")
val infoGain = informationEvaluator.evaluate
```

## Contact
* My blog website: www.iyao.ren.
* Follow me on WeChat:

<div>
<img src="https://i.loli.net/2018/08/17/5b75bc2367e4b.jpg" width = "200" height = "200" alt=""  align=center />
</div>


