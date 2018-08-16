package org.fepipeline.util;

import org.fepipeline.feature.evaluation.SampleStatEntity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import java.util.Arrays;
import java.util.List;

/**
 * DiscretizationUtil.
 *
 * @author Zhang Chaoli
 */
public class DiscretizationUtil {
    /**
     * 等区间（等值）分割: 根据分割的区间个数<code>fieldNum</code>得到<code>continuousValue</code>对应的区间index
     * <p>
     * <b>同类分割,但区分度可能不好</b>
     *
     * @param continuousValue
     * @param fieldNum
     * @return 对应区间索引
     */
    public static int fieldDiscretize(Double continuousValue, int fieldNum) {
        return fieldDiscretize(continuousValue, 0.0, fieldNum, 1.0);
    }

    /**
     * 等区间分割: 根据分割的区间个数<code>fieldNum</code>、分割区间下限值<code>lowbound</code>和分割区间上限值<code>upBound</code>,得到<code>continuousValue</code>对应的区间index
     * <p>
     * <b>同类分割,但区分度可能不好</b>
     *
     * @param continuousValue
     * @param fieldNum
     * @param upBound
     * @return 对应区间索引
     */
    public static int fieldDiscretize(Double continuousValue, Double lowbound, int fieldNum, Double upBound) {
        Double interval = (upBound - lowbound) / fieldNum;
        int idx = 0;
        if (Range.closedOpen(0.0, lowbound).contains(continuousValue)) {
            return idx;
        }
        for (idx = 0; idx < fieldNum; idx++) {
            if (Range.closedOpen(lowbound + interval * idx, lowbound + interval * (idx + 1)).contains(continuousValue)) {
                if (lowbound > 0.0) {
                    return idx + 1;
                } else {
                    return idx;
                }
            }
        }
        if (lowbound > 0.0) {
            return fieldNum + 1;
        } else {
            return fieldNum;
        }
    }

    /**
     * 等量分割
     * <p>
     * <b>区分度好,但同区间可能不同类</b>
     *
     * @param continuousValue
     * @param quantileVec     分位数向量（包括0分位数）
     * @return 对应区间索引（从0开始）
     */
    public static <C extends Comparable<C>> int boundsDiscretize(C continuousValue, C[] quantileVec) {
        return boundsDiscretize(continuousValue, Arrays.asList(quantileVec));
    }

    /**
     * 等量分割
     * <p>
     * <b>区分度好,但同区间可能不同类</b>
     *
     * @param continuousValue
     * @param quantileVec     分位数向量（包括0分位数）
     * @return 对应区间索引（从0开始）
     */
    public static <C extends Comparable<C>> int boundsDiscretize(C continuousValue, List<C> quantileVec) {
        for (int idx = 0; idx < quantileVec.size() - 1; idx++) {
            if (Range.closedOpen(quantileVec.get(idx), quantileVec.get(idx + 1)).contains(continuousValue)) {
                return idx;
            }
        }
        if (continuousValue.compareTo(quantileVec.get(0)) < 0) {
            return -1;
        }
        return quantileVec.size() - 1;
    }

    public static int boundsDiscretize(Double continuousValue, List<Double> quantileVec) {
        for (int idx = 0; idx < quantileVec.size() - 1; idx++) {
            if (Range.closedOpen(quantileVec.get(idx), quantileVec.get(idx + 1)).contains(continuousValue)) {
                return idx;
            }
        }
        if (continuousValue.compareTo(quantileVec.get(0)) < 0) {
            return -1;
        }
        return quantileVec.size() - 1;
    }

    public static double sameNumFractiles(double[] data, double p) {
        int n = data.length;
        Arrays.sort(data);
        double px = p * (n - 1);
        int i = (int) Math.floor(px);
        double g = px - i;
        if (g == 0) {
            return data[i];
        } else {
            return (1 - g) * data[i] + g * data[i + 1];
        }
    }

    /**
     * 计算信息增益
     * E(Source) - sum(p/t * I(pi/ti, ni/ti));
     *
     * @param samples
     * @return a double value for the Info Gain
     */
    public static <T> double getInfoGain(List<SampleStatEntity<T>> samples) {
        double sampleNum = 0.0;
        double positiveNum = 0.0;
        double negativeNum = 0.0;
        for (SampleStatEntity sample : samples) {
            sampleNum += sample.getAllNum();
            positiveNum += sample.getPositiveNum();
            negativeNum += sample.getNegativeNum();
        }

        if (sampleNum == 0.0) {
            throw new RuntimeException("The sample number is zero.");
        }

        double entropySource = entropy(positiveNum / sampleNum, negativeNum / sampleNum);
        double entropyAttribute = 0.0;
        for (SampleStatEntity sample : samples) {
            entropyAttribute += (sample.getAllNum() / sampleNum) * entropy((double) sample.getPositiveNum() / sample.getAllNum(), (double) sample.getNegativeNum() / sample.getAllNum());
        }
        return entropySource - entropyAttribute;
    }

    /**
     * 计算向量的信息熵
     * sum(-p(vi)logbase2ofp(vi))
     *
     * @param ds
     * @return entropy value of certain Attribute
     */
    private static double entropy(Double... ds) {
        double finalValue = 0;
        for (double d : ds) {
            if (d != 0.0) {
                finalValue += -d * Math.log(d) / Math.log(2.0);
            }
        }

        if (Double.isNaN(finalValue)) {
            finalValue = 0;
        }

        return finalValue;
    }


    public static void main(String[] args) {
//        System.out.println(fieldDiscretize(0.91, 10));
        System.out.println(fieldDiscretize(0.8, 0.01, 10, 0.1));

//        int[] array = {0, 1, 2, 4, 8, 20, 39, 130};
//        System.out.println(boundsDiscretize(53, ArrayUtils.toObject(array)));
        List<Double> list = ImmutableList.of(0.0, 0.21, 0.35, 0.56);
        System.out.println(boundsDiscretize(0.44, list));
//        System.out.println(Range.closedOpen(0, 0).contains(0));

        DiscretizationUtil discretizationUtil = new DiscretizationUtil();
        discretizationUtil.fieldDiscretize(0.8, 0.01, 10, 0.1);
    }
}
