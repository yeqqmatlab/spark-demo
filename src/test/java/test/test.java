package test;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yqq on 2019/7/5.
 */
public class test {
    public static void main(String[] args) {
        //根据学科数量划分分数段
        int totalScoreNum = 118;
        List<Integer> ScoreDistributionList = new ArrayList<>();
        int subjectNum = 1;
        if (subjectNum == 1) { // 5分一段
            for(int i = totalScoreNum ; i > 0 ; i = i-5 ){
                ScoreDistributionList.add(i);
            }
        } else if(subjectNum > 1 && subjectNum <= 3){ // 10分一段
            for(int i = totalScoreNum ; i > 0 ; i = i-10 ){
                ScoreDistributionList.add(i);
            }
        }else if(subjectNum > 3){ // 20分一段
            for(int i = totalScoreNum ; i > 0 ; i = i-20 ){
                ScoreDistributionList.add(i);
            }
        }


        System.out.println();
        ScoreDistributionList.forEach(System.out::println);
    }
}

