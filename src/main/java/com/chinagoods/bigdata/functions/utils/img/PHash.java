package com.chinagoods.bigdata.functions.utils.img;

import com.chinagoods.bigdata.functions.utils.img.domain.CImage;
import com.chinagoods.bigdata.functions.utils.img.domain.Digest;
import com.chinagoods.bigdata.functions.utils.img.domain.Features;
import com.chinagoods.bigdata.functions.utils.img.domain.Projections;

import java.util.Arrays;

/**
 * @author Tommy Lee
 * @description Phash 算法实现
 **/
public class PHash {
    private static final double SQRT_TWO = Math.sqrt(2);
    private static final int UCHAR_MAX = 255;
    private static final double[] THETA_180;
    private static final double[] TAN_THETA_180;

    static {
        THETA_180 = new double[180];
        TAN_THETA_180 = new double[180];
        for (int i = 0; i < 180; i++) {
            THETA_180[i] = i * Math.PI / 180;
            TAN_THETA_180[i] = Math.tan(THETA_180[i]);
        }
    }

    public boolean phRadonProjections(CImage img, int N, Projections projs) {
        int width = img.width;
        int height = img.height;
        int D = Math.max(width, height);
        int xOff = (width >> 1) + (width & 0x1);
        int yOff = (height >> 1) + (height & 0x1);
        projs.r = new CImage(N, D);
        projs.nbPixPerline = new int[N];
        projs.size = N;

        Arrays.fill(projs.nbPixPerline, 0);

        CImage ptrRadonMap = projs.r;
        int[] nbPerLine = projs.nbPixPerline;

        for (int k = 0; k < N / 4 + 1; k++) {
            double alpha = TAN_THETA_180[k];
            for (int x = 0; x < D; x++) {
                double y = alpha * (x - xOff);
                int yd = (int) Math.floor(y + (y >= 0 ? 0.5 : -0.5));
                if ((yd + yOff >= 0) && (yd + yOff < height) && (x < width)) {
                    ptrRadonMap.data[k + x * N] = img.data[x
                            + ((yd + yOff) * width)];
                    nbPerLine[k]++;
                }

                if ((yd + xOff >= 0) && (yd + xOff < width) && (k != N / 4)
                        && (x < height)) {
                    ptrRadonMap.data[(N / 2 - k) + x * N] = img.data[(yd + xOff)
                            + x * width];
                    nbPerLine[N / 2 - k]++;
                }
            }
        }

        int j = 0;
        for (int k = 3 * N / 4; k < N; k++) {
            double alpha = TAN_THETA_180[k];
            for (int x = 0; x < D; x++) {
                double y = alpha * (x - xOff);
                int yd = (int) Math.floor(y + (y >= 0 ? 0.5 : -0.5));
                if ((yd + yOff >= 0) && (yd + yOff < height) && (x < width)) {
                    ptrRadonMap.data[k + x * N] = img.data[x
                            + ((yd + yOff) * width)];
                    nbPerLine[k]++;
                }

                if ((yOff - yd >= 0) && (yOff - yd < width)
                        && (2 * yOff - x >= 0) && (2 * yOff - x < height)
                        && (k != 3 * N / 4)) {
                    ptrRadonMap.data[(k - j) + x * N] = img.data[(-yd + yOff)
                            + (-(x - yOff) + yOff) * width];
                    nbPerLine[k - j]++;
                }

            }

            j = j + 2;

        }

        return true;

    }

    public boolean phFeatureVector(Projections projs, Features fv) {
        CImage projectionMap = projs.r;
        int[] nbPerline = projs.nbPixPerline;
        int N = projs.size;
        int D = projectionMap.height;
        fv.features = new double[N];

        Arrays.fill(fv.features, 0);

        fv.size = N;

        double[] featV = fv.features;
        double sum = 0.0;
        double sumSqd = 0.0;

        for (int k = 0; k < N; k++) {
            double lineSum = 0.0;
            double lineSumSqd = 0.0;
            int nbPixels = nbPerline[k];
            for (int i = 0; i < D; i++) {
                lineSum += projectionMap.data[k + (i * projectionMap.width)] & 0xFF;
                lineSumSqd += (projectionMap.data[k
                        + (i * projectionMap.width)] & 0xFF)
                        * (projectionMap.data[k + (i * projectionMap.width)] & 0xFF);
            }
            featV[k] = (lineSumSqd / nbPixels) - (lineSum * lineSum)
                    / (nbPixels * nbPixels);
            sum += featV[k];
            sumSqd += featV[k] * featV[k];
        }

        double mean = sum / N;
        double var = Math.sqrt((sumSqd / N) - (sum * sum) / (N * N));

        for (int i = 0; i < N; i++) {
            featV[i] = (featV[i] - mean) / var;
        }

        return true;
    }

    public boolean phDct(Features fv, Digest digest) {
        int N = fv.size;
        int nbCoeffs = 40;

        digest.coeffs = new int[nbCoeffs];
        digest.size = nbCoeffs;

        double[] R = fv.features;
        int[] D = digest.coeffs;

        double[] dTemp = new double[nbCoeffs];

        double max = 0.0;
        double min = 0.0;

        for (int k = 0; k < nbCoeffs; k++) {
            double sum = 0.0;
            for (int i = 0; i < N; i++) {
                double temp = R[i]
                        * Math.cos((Math.PI * (2 * i + 1) * k) / (2 * N));
                sum += temp;
            }
            if (k == 0) {
                dTemp[k] = sum / Math.sqrt((double) N);
            } else {
                dTemp[k] = sum * SQRT_TWO / Math.sqrt((double) N);
            }
            if (dTemp[k] > max) {
                max = dTemp[k];
            }
            if (dTemp[k] < min) {
                min = dTemp[k];
            }
        }

        for (int i = 0; i < nbCoeffs; i++) {
            D[i] = (int) (UCHAR_MAX * (dTemp[i] - min) / (max - min));
        }

        return true;

    }

    public boolean phImageDigest(CImage img, Digest digest, int n) {
        img.blur();
        Projections projs = new Projections();
        phRadonProjections(img, n, projs);
        Features features = new Features();
        phFeatureVector(projs, features);
        phDct(features, digest);
        return true;
    }

    public double phCrossCorr(Digest x, Digest y) {

        int n = y.size;

        int[] xCoeffs = x.coeffs;
        int[] yCoeffs = y.coeffs;

        double[] r = new double[n];
        double sumx = 0.0;
        double sumy = 0.0;
        for (int i = 0; i < n; i++) {
            sumx += xCoeffs[i];
            sumy += yCoeffs[i];
        }

        double meanx = sumx / n;
        double meany = sumy / n;
        double max = 0;

        for (int d = 0; d < n; d++) {
            double num = 0.0;
            double denx = 0.0;
            double deny = 0.0;
            for (int i = 0; i < n; i++) {
                num += (xCoeffs[i] - meanx)
                        * (yCoeffs[(n + i - d) % n] - meany);
                denx += Math.pow((xCoeffs[i] - meanx), 2);
                deny += Math.pow((yCoeffs[(n + i - d) % n] - meany), 2);
            }
            r[d] = num / Math.sqrt(denx * deny);
            if (r[d] > max) {
                max = r[d];
            }
        }

        return max;
    }

    public double phCompareImages(CImage imA, CImage imB) {
        int n = 180;

        Digest digestA = new Digest();
        phImageDigest(imA, digestA, n);

        Digest digestB = new Digest();
        phImageDigest(imB, digestB, n);

        double pcc = phCrossCorr(digestA, digestB);
        return pcc;
    }

    public static void main(String[] args) {
        PHash phash = new PHash();
        CImage imA = new CImage("e:\\liuyifei.jpeg");
        int n = 180;
        Digest digestA = new Digest();
        phash.phImageDigest(imA, digestA, n);

        CImage imB = new CImage("e:\\liuyifei.jpeg");
        Digest digestB = new Digest();
        phash.phImageDigest(imB, digestB, n);

        System.out.println(phash.phCompareImages(imA, imB));

//        1368921489
        System.out.println(Arrays.hashCode(digestA.coeffs));
        System.out.println(Arrays.hashCode(digestB.coeffs));
    }
}
