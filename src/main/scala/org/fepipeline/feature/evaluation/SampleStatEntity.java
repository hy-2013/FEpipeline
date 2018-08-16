package org.fepipeline.feature.evaluation;

import lombok.Getter;

/**
 * SampleStatEntity.
 *
 * @author Zhang Chaoli
 */
public class SampleStatEntity<T> {
    /**
     * 样本属性值的类别
     */
    @Getter
    private T attributeValue;
    /**
     * 该属性值的总数
     */
    @Getter
    private Long allNum;
    /**
     * 该属性值的正样本总数
     */
    @Getter
    private Long positiveNum;
    /**
     * 该属性值的负样本总数
     */
    @Getter
    private Long negativeNum;

    public SampleStatEntity(T attributeValue, Long allNum, Long positiveNum, Long negativeNum) {
        this.attributeValue = attributeValue;
        this.allNum = allNum;
        this.positiveNum = positiveNum;
        this.negativeNum = negativeNum;
    }

    @Override
    public String toString() {
        return "SampleStatEntity{" +
                "attributeValue=" + attributeValue +
                ", allNum=" + allNum +
                ", positiveNum=" + positiveNum +
                ", negativeNum=" + negativeNum +
                '}';
    }
}
