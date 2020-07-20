package org.apache.flink.streaming.examples;

public class DemoCode {

    static void quickSort(int[] arr, int begin, int end) {
        if (begin < end) {
            int temp = arr[begin];
            int i = begin;
            int j = end;
            while (i < j) {
                while (arr[i] < temp &&  i < end) {
                    i++;
                }
                while (arr[j] > temp && j > begin ) {
                    j--;
                }
                if (i <= j) {
                    int tt = arr[i];
                    arr[i] = arr[j];
                    arr[j] = tt;
                    i++;
                    j--;
                }
            }
            if (begin < j) {
                quickSort(arr, begin, j);
            }
            if (i < end) {
                quickSort(arr, i, end);
            }
        }
    }
}
