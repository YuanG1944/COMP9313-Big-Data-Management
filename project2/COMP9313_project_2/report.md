### Yuan Gao z5239220 Report

### 1. Evaluation of stacking model on the test data
>1.1 Build a Preprocessing Pipeline  
>
>+ Process `train_data`, use `pipline` increase `Tokenizer`, `CountVectorizer` and `StringIndexer` columns
>
><img src="/Users/yuan/Library/Application Support/typora-user-images/image-20200804123026291.png" alt="image-20200804123026291" style="zoom:50%;" />    
>
>1.2 Generate Meta Features for Training
>+ Generate Meta Features:  
><img src="/Users/yuan/Library/Application Support/typora-user-images/image-20200804125915127.png" alt="image-20200804125915127" style="zoom:67%;" /> 
>+ For every Feature:
>
><img src="/Users/yuan/Library/Application Support/typora-user-images/image-20200804131817702.png" alt="image-20200804131817702" style="zoom:50%;" />  
>
>+ Combine 9 Column:
>
><img src="/Users/yuan/Desktop/截屏2020-08-04 下午1.25.44.png" alt="截屏2020-08-04 下午1.25.44" style="zoom:60%;" /> 
>
>1.3 Obtain the prediction for the test data  
>
>+ Prediction
>> Using meta Features to predict 
>
> <img src="/Users/yuan/Library/Application Support/typora-user-images/image-20200804202912078.png" alt="image-20200804202912078" style="zoom:50%;" /> 
>
>
>### 2. How to improve the performance  
>
>  In `pyspark.ml.feature`, there are `RegexTokenizer` function can address punctuation. We can use this function to reduce the effect of nonsense character. Moreover, we can add Decision Tree model geting `dt_pred_0, dt_pred_1, dt_pred_2` and compose with `NB` and `SVM`. Nine columns geting meta feature can improve the performance of the stacking model.