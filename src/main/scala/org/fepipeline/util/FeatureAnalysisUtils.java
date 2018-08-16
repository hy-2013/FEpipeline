package org.fepipeline.util;

import org.fepipeline.feature.evaluation.SampleStatEntity;
import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * FeatureAnalysisUtils.
 *
 * @author Zhang Chaoli
 */
@Slf4j
public class FeatureAnalysisUtils {
    /**
     * max-min归一化向量vec
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> List<Double> normalize(List<T> vec, Class<T> clazz) {
        List<Double> vecNormlized = Lists.newArrayList();

        if (clazz == Double.class) {
            Double min = Double.MAX_VALUE;
            Double max = Double.MIN_VALUE;
            for (Number n : vec) {
                if (min > n.doubleValue()) {
                    min = n.doubleValue();
                }
                if (max < n.doubleValue()) {
                    max = n.doubleValue();
                }
            }
            for (Number n : vec) {
                double normlized = (n.doubleValue() - min) / (max - min);
                vecNormlized.add(normlized);
            }
            return vecNormlized;
        }
        if (clazz == Integer.class) {
            Integer min = Integer.MAX_VALUE;
            Integer max = Integer.MIN_VALUE;
            for (Number n : vec) {
                if (min > n.intValue()) {
                    min = n.intValue();
                }
                if (max < n.intValue()) {
                    max = n.intValue();
                }
            }
            for (Number n : vec) {
                double normlized = (double)(n.intValue() - min) / (max - min);
                vecNormlized.add(normlized);
            }
            return vecNormlized;
        }
        if (clazz == Long.class) {
            Long min = Long.MAX_VALUE;
            Long max = Long.MIN_VALUE;
            for (Number n : vec) {
                if (min > n.longValue()) {
                    min = n.longValue();
                }
                if (max < n.longValue()) {
                    max = n.longValue();
                }
            }
            for (Number n : vec) {
                double normlized = (double)(n.longValue() - min) / (max - min);
                vecNormlized.add(normlized);
            }
            return vecNormlized;
        }
        if (clazz == Float.class) {
            Float min = Float.MAX_VALUE;
            Float max = Float.MIN_VALUE;
            for (Number n : vec) {
                if (min > n.floatValue()) {
                    min = n.floatValue();
                }
                if (max < n.floatValue()) {
                    max = n.floatValue();
                }
            }
            for (Number n : vec) {
                double normlized = (double)(n.floatValue() - min) / (max - min);
                vecNormlized.add(normlized);
            }
            return vecNormlized;
        }
        if (clazz == Short.class) {
            Short min = Short.MAX_VALUE;
            Short max = Short.MIN_VALUE;
            for (Number n : vec) {
                if (min > n.shortValue()) {
                    min = n.shortValue();
                }
                if (max < n.shortValue()) {
                    max = n.shortValue();
                }
            }
            for (Number n : vec) {
                double normlized = (double)(n.shortValue() - min) / (max - min);
                vecNormlized.add(normlized);
            }
            return vecNormlized;
        }

        log.warn("The input List type is not {Double, Float, Integer, Long, Short}.");
        return null;
    }

    /**
     * max-min归一化向量vec
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> List<Double> normalize(T[] vec, Class<T> clazz) {
        return normalize(Arrays.asList(vec), clazz);
    }

    /**
     * 计算向量vec的均值
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getMean(List<T> vec, Class<T> clazz) {
        int size = vec.size();
        double sum = 0;
        if (clazz == Double.class) {
            for (Number n : vec) {
                sum += n.doubleValue();
            }
            return sum / size;
        }
        if (clazz == Integer.class) {
            for (Number n : vec) {
                sum += n.intValue();
            }
            return sum / size;
        }
        if (clazz == Long.class) {
            for (Number n : vec) {
                sum += n.longValue();
            }
            return sum / size;
        }
        if (clazz == Float.class) {
            for (Number n : vec) {
                sum += n.floatValue();
            }
            return sum / size;
        }
        if (clazz == Short.class) {
            for (Number n : vec) {
                sum += n.shortValue();
            }
            return sum / size;
        }

        log.warn("The input List type is not {Double, Float, Integer, Long, Short}.");
        return Double.MAX_VALUE;
    }

    /**
     * 计算向量vec的方差
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getVariance(List<T> vec, Class<T> clazz) {
        double avg = getMean(vec, clazz);
        return getVariance(vec, avg, clazz);
    }

    /**
     * 计算向量vec的方差
     * @param vec
     * @param avg
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getVariance(List<T> vec, Double avg, Class<T> clazz) {
        int size = vec.size();
        double varianceTmp = 0;
        if (clazz == Double.class) {
            for (Number n : vec) {
                varianceTmp += (n.doubleValue() - avg) * (n.doubleValue() - avg);
            }
            return varianceTmp / size;
        }
        if (clazz == Integer.class) {
            for (Number n : vec) {
                varianceTmp += (n.intValue() - avg) * (n.intValue() - avg);
            }
            return varianceTmp / size;
        }
        if (clazz == Long.class) {
            for (Number n : vec) {
                varianceTmp += (n.longValue() - avg) * (n.longValue() - avg);
            }
            return varianceTmp / size;
        }
        if (clazz == Float.class) {
            for (Number n : vec) {
                varianceTmp += (n.floatValue() - avg) * (n.floatValue() - avg);
            }
            return varianceTmp / size;
        }
        if (clazz == Short.class) {
            for (Number n : vec) {
                varianceTmp += (n.shortValue() - avg) * (n.shortValue() - avg);
            }
            return varianceTmp / size;
        }

        log.warn("The input List type is not {Double, Float, Integer, Long, Short}.");
        return Double.MAX_VALUE;
    }

    /**
     * 计算向量vec的方差
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getVariance(T[] vec, Class<T> clazz) {
        return getVariance(Arrays.asList(vec), clazz);
    }

    /**
     * 计算向量vec的标准差
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getStandardDeviation(List<T> vec, Class<T> clazz) {
        return Math.sqrt(getVariance(vec, clazz));
    }

    /**
     * 计算向量vec的标准差
     * @param vec
     * @param avg
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getStandardDeviation(List<T> vec, Double avg, Class<T> clazz) {
        return Math.sqrt(getVariance(vec, avg, clazz));
    }

    /**
     * 计算向量vec的标准差
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getStandardDeviation(T[] vec, Class<T> clazz) {
        return getStandardDeviation(Arrays.asList(vec), clazz);
    }

    /**
     * 计算向量vec的变异系数, 当平均值接近于0的时候,不宜使用
     * 标准差/均值
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getCoefficientOfVariation(List<T> vec, Class<T> clazz) {
        double avg = getMean(vec, clazz);
        if (Math.abs(avg) < 0.001) {
            log.warn("Since avg is little can 0.001, please check out of whether is proper to use Coefficient of Variation");
        }
        double variance = getStandardDeviation(vec, avg, clazz);
        return variance / avg;
    }

    /**
     * 计算向量vec的变异系数, 当平均值接近于0的时候,不宜使用
     * 标准差/均值
     * @param vec
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T extends Number> double getCoefficientOfVariation(T[] vec, Class<T> clazz) {
        return getCoefficientOfVariation(Arrays.asList(vec), clazz);
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
            log.warn("The sample number is zero.");
            return Double.MIN_VALUE;
        }

        double entropySource = entropy(positiveNum / sampleNum, negativeNum / sampleNum);
        double entropyAttribute = 0.0;
        for (SampleStatEntity sample : samples) {
            entropyAttribute += (sample.getAllNum() / sampleNum) * entropy((double) sample.getPositiveNum() / sample.getAllNum(), (double) sample.getNegativeNum() / sample.getAllNum());
        }
        return entropySource - entropyAttribute;
    }

    /**
     * 计算信息增益率
     * InfoGain / E(info);
     *
     * @param samples
     * @return a double value for the Info Gain-Ratio
     */
    public static <T>  double getInfoGainRatio(List<SampleStatEntity<T>> samples) {
        double infoGain = getInfoGain(samples);
        return getInfoGainRatio(samples, infoGain);
    }

    /**
     * 计算信息增益率
     * InfoGain / E(info);
     *
     * @param samples
     * @param infoGain
     * @param <T>
     * @return
     */
    public static <T>  double getInfoGainRatio(List<SampleStatEntity<T>> samples, Double infoGain) {
        double sampleNum = 0.0;
        for (SampleStatEntity sample : samples) {
            sampleNum += sample.getAllNum();
        }
        double infoEntropy = 0.0;
        for (SampleStatEntity sample : samples) {
            infoEntropy += entropy(sample.getAllNum() / sampleNum);
        }
        return infoGain / infoEntropy;
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

    /**
     * 读取样本统计文件,并将其转化为sample list,以便计算信息增益或信息增益率
     * @param path
     * @return
     */
    public static List<SampleStatEntity<String>> readFile2SampleList(String path) {
        return readFile2SampleList(path, "\t", String.class);
    }

    /**
     * 读取样本统计文件,并将其转化为sample list,以便计算信息增益或信息增益率
     * @param path
     * @param attributeValueIdx
     * @param allNumIdx
     * @param positiveNumIdx
     * @param negativeNumIdx
     * @return
     */
    public static List<SampleStatEntity<String>> readFile2SampleList(String path, final int attributeValueIdx, final int allNumIdx, final int positiveNumIdx, final int negativeNumIdx) {
        return readFile2SampleList(path, "\t", String.class, attributeValueIdx, allNumIdx, positiveNumIdx, negativeNumIdx);
    }

    /**
     * 读取样本统计文件,并将其转化为sample list,以便计算信息增益或信息增益率
     * @param path
     * @param separator 定义分隔符
     * @return
     */
    public static List<SampleStatEntity<String>> readFile2SampleList(String path, String separator) {
        return readFile2SampleList(path, separator, String.class);
    }


    /**
     * 取样本统计文件,并将其转化为sample list,以便计算信息增益或信息增益率
     * @param path
     * @param separator
     * @param attributeValueIdx
     * @param allNumIdx
     * @param positiveNumIdx
     * @param negativeNumIdx
     * @return
     */
    public static List<SampleStatEntity<String>> readFile2SampleList(String path, String separator, final int attributeValueIdx, final int allNumIdx, final int positiveNumIdx, final int negativeNumIdx) {
        return readFile2SampleList(path, separator, String.class, attributeValueIdx, allNumIdx, positiveNumIdx, negativeNumIdx);
    }

    /**
     * 读取样本统计文件,并将其转化为sample list,以便计算信息增益或信息增益率
     * @param path
     * @param clazz 样本属性值的类别
     * @param <T>
     * @return
     */
    public static <T> List<SampleStatEntity<T>> readFile2SampleList(String path, Class<T> clazz) {
        return readFile2SampleList(path, "\t", clazz);
    }

    /**
     * 读取样本统计文件,并将其转化为sample list,以便计算某个Attribute的信息增益或信息增益率
     * 默认文件格式: < attributeID, attributeName, allNum, positiveNum, negativeNum>
     * @param path
     * @param separator 定义分隔符
     * @param clazz 样本属性值的类别
     * @param <T>
     * @return
     */
    public static <T> List<SampleStatEntity<T>> readFile2SampleList(String path, String separator, Class<T> clazz){
        return readFile2SampleList(path, separator, clazz, 0, 2, 3, 4);
    }

    /**
     * 读取样本统计文件,并将其转化为sample list,以便计算信息增益或信息增益率
     * @param path
     * @param separator 定义分隔符
     * @param clazz 样本属性值的类别
     * @param attributeValueIdx
     * @param allNumIdx
     * @param positiveNumIdx
     * @param negativeNumIdx
     * @param <T>
     * @return
     */
    public static <T> List<SampleStatEntity<T>> readFile2SampleList(String path, String separator, Class<T> clazz, final int attributeValueIdx, final int allNumIdx, final int positiveNumIdx, final int negativeNumIdx){
        if (StringUtils.isBlank(path)) {
            log.warn("SampleStatEntity file path is blank.");
            return null;
        }

        List<SampleStatEntity<T>> sampleList = Lists.newArrayList();
        File file = new File(path);
        try {
            List<String> lines = Files.readLines(file, Charsets.UTF_8);
            for (String line : lines) {
                String[] arr = line.split(separator, -1);
                T attributeValue = clazz.cast(arr[attributeValueIdx]);
                Long allNum = Long.parseLong(arr[allNumIdx].replace(",", "").trim());
                Long positiveNum = Long.parseLong(arr[positiveNumIdx].replace(",", "").trim());
                Long negativeNum = Long.parseLong(arr[negativeNumIdx].replace(",", "").trim());
                sampleList.add(new SampleStatEntity<T>(attributeValue, allNum, positiveNum, negativeNum));
            }
        } catch (IndexOutOfBoundsException obe) {
            log.warn("The input sample file format is error. ", obe);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sampleList;
    }

    public static <T> List<T> readFile2SampleList(String path, String separator, final int idx, Class<T> clazz){
        if (StringUtils.isBlank(path)) {
            log.warn("SampleStatEntity file path is blank.");
            return null;
        }

        List<T> sampleList = Lists.newArrayList();
        File file = new File(path);
        try {
            List<String> lines = Files.readLines(file, Charsets.UTF_8);
            for (String line : lines) {
                String[] arr = line.split(separator, -1);
                T attributeValue = clazz.cast(Double.parseDouble(arr[idx]));
                sampleList.add(attributeValue);
            }
        } catch (IndexOutOfBoundsException obe) {
            log.warn("The input sample file format is error. ", obe);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sampleList;
    }

    /**
     * 读取样本文件,并将其转化为sample list,以便计算某个Attribute的信息增益或信息增益率
     * @param path
     * @param separator 定义分隔符
     * @param clazz 样本属性值的类别
     * @param attributeValueIdx
     * @param labelIdx 规定正样本为1,负样本为0
     * @param <T>
     * @return
     */
    public static <T> List<SampleStatEntity<T>> readSourceFile2SampleList(String path, String separator, Class<T> clazz, final int attributeValueIdx, final int labelIdx) {
        if (StringUtils.isBlank(path)) {
            log.warn("SampleStatEntity file path is blank.");
            return null;
        }

        List<SampleStatEntity<T>> sampleList = Lists.newArrayList();
        ListMultimap<T, Byte> multimap = ArrayListMultimap.create();
        File file = new File(path);
        try {
            List<String> lines = Files.readLines(file, Charsets.UTF_8);
            for (String line : lines) {
                String[] arr = line.split(separator, -1);
                T attributeValue = clazz.cast(arr[attributeValueIdx]);
                Byte label = Byte.parseByte(arr[labelIdx]);
                multimap.put(attributeValue, label);
            }
            for (T attributeValue : multimap.keySet()) {
                List<Byte> labelList = multimap.get(attributeValue);
                long allNum = labelList.size();
                long positiveNum = 0L;
                for (Byte b : labelList) {
                    positiveNum += b;
                }
                sampleList.add(new SampleStatEntity<T>(attributeValue, allNum, positiveNum, allNum-positiveNum));
            }
        } catch (IndexOutOfBoundsException obe) {
            log.warn("The input source sample file format is error. ", obe);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sampleList;
    }

}
