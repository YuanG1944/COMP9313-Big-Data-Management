from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StringIndexer, CountVectorizer

def joinfuc(nbN, svmN):
    if nbN == 0.0 and svmN == 0.0:
        return 0.0
    elif nbN == 0.0 and svmN == 1.0:
        return 1.0
    elif nbN == 1.0 and svmN == 0.0:
        return 2.0
    else:
        return 3.0

def base_features_gen_pipeline(input_descript_col="descript", input_category_col="category", output_feature_col="features", output_label_col="label"):
    token = Tokenizer(inputCol=input_descript_col, outputCol="content")
    countVec = CountVectorizer(inputCol="content", outputCol=output_feature_col)
    indexer = StringIndexer(inputCol=input_category_col, outputCol=output_label_col)
    return Pipeline(stages=[token, countVec, indexer])

def gen_meta_features(training_df, nb_0, nb_1, nb_2, svm_0 , svm_1, svm_2):
    resDF = None
    numGroup = training_df.select("group").dropDuplicates().count()
    for value in range(numGroup):
        strVal = str(value)
        trainingSet = training_df.filter("group!=" + strVal)
        currSet = training_df.filter("group=" + strVal)

        ml_pip = Pipeline(stages=[nb_0, nb_1, nb_2, svm_0, svm_1, svm_2])
        if not resDF:
            resDF = ml_pip.fit(trainingSet).transform(currSet)
        else:
            resDF = resDF.union(ml_pip.fit(trainingSet).transform(currSet))
    joinValue = udf(joinfuc, DoubleType())
    res = resDF.withColumn("joint_pred_0", joinValue("nb_pred_0", "svm_pred_0"))
    res = res.withColumn("joint_pred_1", joinValue("nb_pred_1", "svm_pred_1"))
    res = res.withColumn("joint_pred_2", joinValue("nb_pred_2", "svm_pred_2"))
    return res
    

def test_prediction(test_df, base_features_pipeline_model, gen_base_pred_pipeline_model, gen_meta_feature_pipeline_model, meta_classifier):
    testDF = base_features_pipeline_model.transform(test_df)
    testDF = gen_base_pred_pipeline_model.transform(testDF)
    joinValue = udf(joinfuc, DoubleType())
    testDF = testDF.withColumn("joint_pred_0", joinValue("nb_pred_0", "svm_pred_0")) 
    testDF = testDF.withColumn("joint_pred_1", joinValue("nb_pred_1", "svm_pred_1"))
    testDF = testDF.withColumn("joint_pred_2", joinValue("nb_pred_2", "svm_pred_2"))
    featuresDF = gen_meta_feature_pipeline_model.transform(testDF)
    res = meta_classifier.transform(featuresDF).select("id", "label", "final_prediction")
    return res