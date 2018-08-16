# FEpipeline
A easy to get start, scalable, distributed feature engineering library based on Spark.

## FEpipeline Framework
![](https://i.loli.net/2018/08/17/5b75bc2363823.jpg)

## Modules
* SubSampler: Subsample samples or raw samples as defined strategy (strategies), such as OneEqualOneSubsampler, ManyContainOneSubsampler, RandomSubsampler, SkipAboveSubsampler, UserSubsampler, NegativeSubsampler etc.
* Convertor: Convert String or Array or any other non-numeric feature to numeric feature, even do some normalized conversion. Such as TitleLenConvertor, WelCntConvertor etc.
* Assembler: Assemble  features to a feature. It can be used to make cross features manually.
* Stater: Get feature statistics attribute value by Aggregater, CXRStater (ctr/cvr), BayesianSmoothing etc.
* Discretizer: Discretize feature by the same number in each field (SameNumDiscretizer), or by the evenly divided fields (EqualFieldDiscretizer) etc.
* OutputFormater: Define output format such as FtrlOutputFormater, LibsvmOutputFormater etc.
* Evaluator: It is be used to evaluate the feature degree of importance and divided to StatisticsEvaluator and InformationEvaluator. In StatisticsEvaluator, it has Coverage, Correlation, TopNFreq, Quantiles and Chi-square test etc. In InformationEvaluator, it has InfoGain, InfoGainRatio, InfoValue etc.

## Code Snippet

```
val spark = SparkSession
  .builder()
  .master("local[8]")
  .appName("[Huye] PipelineTest")
  .enableHiveSupport()
  .getOrCreate()

val outPath = args(0)
val rawSample = spark.table("raw_sample")

val colNameIndexes = Array("age"-> 1, "score" -> 2, "name_len"-> 3, "age_score_titlelen" -> 4,
  "pv" -> 5, "item_ctr" -> 6, "name_len_disc" -> 7, "item_ctr_disc" -> 8)

val pipeline = Pipeline(Array(
  OneEqualOneSubsampler("app", "platform"),
  TitleLenConvertor("title", "name_len"),
  Assembler(Array("age", "score", "name_len"), "age_score_titlelen"),
  Aggregater("item", "pv"),
  CXRStater("label", "item", "item_ctr"),
  SameNumDiscretizer("name_len", "name_len_disc", 5),
  SameNumDiscretizer("item_ctr", "item_ctr_disc", 10),
  FtrlOutputFormater(colNameIndexes, "sample")))

val pipelineModel = pipeline.fit(rawSample)
val sample = pipelineModel.transform(rawSample)

sample.write.save(outPath)

val statisticsEvaluator = StatisticsEvaluator(rawSample, "age", "coverage")
val coverage = statisticsEvaluator.evaluate
val informationEvaluator = InformationEvaluator(rawSample, "label", "age", "infoGain")
val infoGain = informationEvaluator.evaluate
```

## Contact
* My blog website: www.iyao.ren.
* Follow me on WeChat:

![-w50](https://i.loli.net/2018/08/17/5b75bc2367e4b.jpg)


