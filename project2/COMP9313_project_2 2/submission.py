from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, CountVectorizer, StringIndexer
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType





def base_features_gen_pipeline(input_descript_col="descript", input_category_col="category", output_feature_col="features", output_label_col="label"):
    n_taken = Tokenizer( inputCol=input_descript_col, outputCol="words" )
    n_CoVe = CountVectorizer( inputCol="words", outputCol=output_feature_col )
    n_StIn = StringIndexer( inputCol=input_category_col, outputCol=output_label_col )
    
    res = Pipeline( stages=[n_taken, n_CoVe, n_StIn] )
    return res


def udf_udf(nb, svm):
    t_udf = udf( compare, DoubleType() )
    res = t_udf(nb, svm)
    return res

def compare(nb_pre, svm_pre):

    nb_str = str(int(nb_pre))
    svm_str = str(int(svm_pre))
 
    res = nb_str + svm_str
    res = int (res, 2)
    
    return float(res)


def gen_meta_features(training_df, nb_0, nb_1, nb_2, svm_0, svm_1, svm_2):
    # pass
    num_group = training_df.select( "group" ).distinct().count()
    

    gen_features_df = training_df.filter(training_df["group"] == 0)
    gen_training_df = training_df.filter(training_df["group"] != 0)      
    sum_pipe = Pipeline( stages=[nb_0, nb_1, nb_2, svm_0, svm_1, svm_2] )
    res = sum_pipe.fit( gen_training_df ).transform( gen_features_df )
  
    for i in range(1,num_group):
        gen_features_df = training_df.filter(training_df["group"] == i)
        gen_training_df = training_df.filter(training_df["group"] != i)    
        sum_pipe = Pipeline( stages=[nb_0, nb_1, nb_2, svm_0, svm_1, svm_2] )
        res = res.union( sum_pipe.fit( gen_training_df ).transform( gen_features_df ) )
        

    r_0 = udf_udf("nb_pred_0", "svm_pred_0")
    r_1 = udf_udf("nb_pred_1", "svm_pred_1")    
    r_2 = udf_udf("nb_pred_2", "svm_pred_2")   
    
    res = res.withColumn( "joint_pred_0", r_0 )
    res = res.withColumn( "joint_pred_1", r_1 )
    res = res.withColumn( "joint_pred_2", r_2 )
    
    return res

def test_prediction(test_df, base_features_pipeline_model, gen_base_pred_pipeline_model, gen_meta_feature_pipeline_model, meta_classifier):

    test_base_trans_df = base_features_pipeline_model.transform(test_df)
    test_gen_trans_df = gen_base_pred_pipeline_model.transform(test_base_trans_df)
    r_0 = udf_udf("nb_pred_0", "svm_pred_0")

    r_1 = udf_udf("nb_pred_1", "svm_pred_1")    

    r_2 = udf_udf("nb_pred_2", "svm_pred_2")      
    
    test_withCol_df = test_gen_trans_df.withColumn( "joint_pred_0", r_0 )
    test_withCol_df = test_withCol_df.withColumn( "joint_pred_1", r_1 )
    test_withCol_df = test_withCol_df.withColumn( "joint_pred_2", r_2 )
    featuresDF = gen_meta_feature_pipeline_model.transform(test_withCol_df)
    res = meta_classifier.transform(featuresDF)
    res =res.select("id", "label", "final_prediction")
    return res                                    
    
