package com.chinagoods.bigdata.functions.utils.img.domain;


import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.Arrays;
import java.util.StringJoiner;

/**
 * @author Tommy Lee
 */
public class Digest implements Serializable {
    private static final long serialVersionUID = 1L;

    public char[] id;
    public int[] coeffs;
    public int size;

    public char[] getId() {
        return id;
    }

    public Digest setId(char[] id) {
        this.id = id;
        return this;
    }

    public int[] getCoeffs() {
        return coeffs;
    }

    public Digest setCoeffs(int[] coeffs) {
        this.coeffs = coeffs;
        return this;
    }

    public int getSize() {
        return size;
    }

    public Digest setSize(int size) {
        this.size = size;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Digest)) {
            return false;
        }
        Digest digest = (Digest) o;
        return getSize() == digest.getSize() &&
                Objects.equal(getId(), digest.getId()) &&
                Objects.equal(getCoeffs(), digest.getCoeffs());
    }


    @Override
    public int hashCode() {
        return Objects.hashCode(getCoeffs(), getSize());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Digest.class.getSimpleName() + "[", "]")
                .add("id=" + Arrays.toString(id))
                .add("coeffs=" + Arrays.toString(coeffs))
                .add("size=" + size)
                .toString();
    }
}