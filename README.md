# EmotionAnalysis</br>
analysis the daily emotion trend of a movie by spark </br>　　
先通过SparkStatistic/splitMovieFile把所有记录都按电影id归分的记录文件　</br>　
EmotionAnalysis读取一部电影RDD，并计算得到该电影的日情感得分，结果如下：　</br>　
2015-09-03      22     1；</br>
2015-09-04      0     1；</br>
2015-09-09      0     0；</br>
2015-09-19      1     0；</br>
2015-09-25      0     0；</br>
2015-10-04      6     0；</br>
2015-10-18      0     0；</br>
2015-10-20      4     7；</br>　　
时间　积极分　消极分</br>
思路：  对于每一条评论先根据标点符号或者空格切分为短句，短句情感分由该短句中情感词和程度词决定，累加每一个短句的情感分，得到该评论全分。</br>  
以时间为键，累加所有评分，最终得到日情感分
