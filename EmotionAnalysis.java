package EmotionAnalysis.myAnalysis;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Serializable;
import scala.Tuple2;

import EmotionAnalysis.dataMining.PathConfig;
import EmotionAnalysis.dataMining.SparkEmotionAnalysis;

public class EmotionAnalysis {

	/**
	 * @param args
	 */
	
	static List<String> positiveDict; //积极词典
	static List<String> negativeDict;//消极词典
	static List<String> insufficentDict;//轻微词
	static List<String> ishDict;//略微词
	static List<String> moreDict; //比较词
	static List<String> veryDict; //非常词
	static List<String> mostDict; //最词
	static List<String> inverseDict;  //非词
	static List<String> overDict;  //过于词
	//评论切割符号
	static Pattern markSpliter = Pattern.compile("[  。？！?.!,，　 ]");
	static JavaSparkContext sc ;
	
	
	public EmotionAnalysis(){
        this.positiveDict = new ArrayList<>();
        this.negativeDict = new ArrayList<>();
        this.insufficentDict = new ArrayList<>();
        this.ishDict = new ArrayList<>();
        this.moreDict = new ArrayList<>();
        this.veryDict = new ArrayList<>();
        this.mostDict = new ArrayList<>();
        this.inverseDict = new ArrayList<>();
        this.overDict = new ArrayList<>();
        this.sc = new JavaSparkContext(new SparkConf().setMaster("local").setAppName("EmotionAnalysis"));
        readDict();
	}
	   public static void readDict() {
		   //读取各字典
	        readFile(PathConfig.positiveDictPath, positiveDict);
	        readFile(PathConfig.negativeDictPath, negativeDict);
	        readFile(PathConfig.insufficentDictPath, insufficentDict);
	        readFile(PathConfig.ishDictPath, ishDict);
	        readFile(PathConfig.moreDictPath, moreDict);
	        readFile(PathConfig.veryDictPath, veryDict);
	        readFile(PathConfig.mostDictPath, mostDict);
	        readFile(PathConfig.inverseDictPath, inverseDict);
	        readFile(PathConfig.overDictPath, overDict);
	    }


	    private static void readFile(String path, List<String> list) {
	    	//给定字典位置，完善一条字典链表
	        BufferedReader br = null;
	        try {
	            br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
	            String temp = null;
	            while ((temp = br.readLine()) != null) {
	                list.add(temp);
	            }
	        } catch (IOException ex) {
	            ex.printStackTrace();
	        } finally {
	            try {
	                br.close();
	            } catch (IOException ex) {
	                Logger.getLogger(SparkEmotionAnalysis.class.getName()).log(Level.SEVERE, null, ex);
	            }
	        }
	    }
	    public static void analysis(){
	    	//读取id文件，即一个id对应的电影的所有记录，计算日平均情感
	    	JavaRDD<String> recordsRDD = sc.textFile("/home/yingying/idFile/564ef2bbc72307ab5599ddc4.txt");
	    	//创建【时间：评论】键值对
	    	JavaPairRDD<String, String> timeComPairRDD = recordsRDD.filter(new dataClean()).mapToPair(new createPair());
//	    	for(Tuple2<String, String> s: timeComPairRDD.take(100)){
//	    		System.out.println(s._1 + "       " + s._2);
//	    	}
	    	//创建【时间，数组】键值对，数组：正极情感分，负极情感分
	    	JavaPairRDD<String, Integer[]> timeScorePairRDD = timeComPairRDD.mapValues(new emotion());
	    	dailyEmotion(timeScorePairRDD);
	    }
	    public static void dailyEmotion(JavaPairRDD<String, Integer[]> timeScorePairRDD){
	    	//计算日均情感分
	    	class score implements Serializable{
	    		public int positive;
	    		public int negative;
	    		public score(int one, int two){
	    			this.positive = one;
	    			this.negative = two;
	    		}
	    	}
	    	Function<Integer[], score> createScore = new Function<Integer[], score>(){
	    		public score call(Integer[] array){
	    			return new score(array[0], array[1]);
	    		}
	    	};
	    	Function2<score, Integer[], score> addAndCount = new Function2<score, Integer[], score>(){
	    		public score call(score scoreClass, Integer[] array){
	    			return new score(scoreClass.positive + array[0], scoreClass.negative + array[1]);
	    		}
	    	};
	    	Function2<score, score, score> combine = new Function2<score, score, score>(){
	    		public score call(score one, score two){
	    			return new score(one.positive + two.positive, one.negative + two.negative);
	    		}
	    	};
	    	JavaPairRDD<String, score> timeScoreClassPairRDD = timeScorePairRDD.combineByKey(createScore, addAndCount, combine).sortByKey();
	    	for(Tuple2<String, score> list: timeScoreClassPairRDD.collect()){
	    		System.out.println(list._1 + "      " + list._2.positive + "     " + list._2.negative);
	    	}
	    }
	    
	    
	    static class emotion implements Function<String, Integer[]>{
	    	//计算一条评论的情感
	    	public Integer[] call(String s){
	    		int positive = 0;
	    	   int negative = 0;
	    		//System.out.println(s);
	    	   //１、短句切分
	    		String[] wholeCom = markSpliter.split(s);
	    		for(String shortCom: wholeCom){
	    			//System.out.println(shortCom);
	    			//２、短句情感分计算
	    			int[] results = emotionCalculation(shortCom);
	    			//3、整理分数
	    			int[] regularResult = standarize(results);
	    			//4、短句情感分累加
	    			positive += regularResult[0];
	    			negative += regularResult[1];
	    		}
	    		//System.out.println("final result   " + positive + "   " + negative);
	    		return new Integer[]{positive, negative};
	    	}
			private int[] standarize(int[] result) {
				//整理分数，避免出现负分
				int[] finalResult = new int[]{0, 0};
				if(result[0] < 0){
					if(result[1] > 0){
						finalResult[0] = 0;
						finalResult[1] =  result[1] - result[0];						
					}else{
						finalResult[0] = -1 * result[1];
						finalResult[1] = -1 * result[0];						
					}
				}else{
					if(result[1] > 0){
						finalResult[0] = result[0];
						finalResult[1] =  result[1] ;						
					}else{
						finalResult[0] = result[0] - result[1];
					    finalResult[1] = 0;					
					}
				}
				return finalResult;
			}
			private int[] emotionCalculation(String shortCom) {
				//短句情感分计算
				int[] result = new int[]{0, 0};
				//寻找句中情感词
				List<String> positiveWords = containWord(shortCom, positiveDict);
				List<String> negativeWords = containWord(shortCom, negativeDict);
				if(! positiveWords.isEmpty()){
					//最终得分由情感词与程度词相加得到
					result[0] = positiveWords.size() + degreeJudgment(shortCom);
				}
				if(! negativeWords.isEmpty()){
					result[1] = negativeWords.size() + degreeJudgment(shortCom);
				}
				return result;
			}

			private int degreeJudgment(String str){
				//程度分判断
				int flag = 1;
				int degree = 0;
				if(containWord(str, inverseDict).size() + containWord(str, overDict).size() != 0){
					flag = -1;
				}
//				printDegreeLength(containWord(str, insufficentDict));
//				printDegreeLength(containWord(str, ishDict));
//				printDegreeLength(containWord(str, moreDict));
//				printDegreeLength(containWord(str, veryDict));
//				printDegreeLength(containWord(str, mostDict));
				  degree += containWord(str, insufficentDict).size() + 2 * containWord(str, ishDict).size()
						+ 3 * containWord(str, moreDict).size() + 4 * containWord(str, veryDict).size()
						+ 5 * containWord(str, mostDict).size();
				return flag * degree;
			}
			private void printDegreeLength(List<String> containList){
				if(containList.size() != 0){
					for(String word: containList)
						System.out.print(word + "   ");					
				
				System.out.print("length is " + containList.size());
				System.out.print("\n");
				}
			}
			private List<String> containWord(String str, List<String> Dict) {
				//判断字符串中是否有字典中词
				List<String> words = new ArrayList<String>();
				for(String s: Dict){
					if((str.contains(s) ) && ( !words.contains(s))){
						words.add(s);
					}
				}
				return words;
			}
	    }
	    

	    static class dataClean implements Function<String, Boolean>{
	    	//数据清理：确保时间及评论非空
	    	public Boolean call(String s){
	    		String[] array = s.split(",");
	    		if((array[1].length() == 24) && (!array[2].equals(""))){
	    			return true;
	    		}
	    		else return false;
	    	}
	    }
	    
	    static class createPair implements PairFunction<String, String, String>{
	    	//创建【时间，评论】键值对
	    	public Tuple2<String, String> call(String s){
	    		return new Tuple2<String, String>(s.split(",")[1].substring(0, 10), s.split(",")[2]);
	    		}
	    	}
	    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		EmotionAnalysis e = new EmotionAnalysis();
		analysis();
	}

}
