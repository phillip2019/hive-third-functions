package com.chinagoods.bigdata.functions.risk.negative;

import java.util.List;

/**
 * @author xiaowei.song
 * @version v1.0.0
 * @description 高效负向词匹配，使用trie算法实现，构建负向词词库，
 * @date 2023/9/6 10:06
 */
public class NegativeStringMatcher {
    private TrieNode root;

    public NegativeStringMatcher(List<String> negativeStrings) {
        root = new TrieNode();
        for (String negativeString : negativeStrings) {
            insert(negativeString);
        }
    }

    public void insert(String word) {
        TrieNode current = root;
        for (char ch : word.toCharArray()) {
            current.children.putIfAbsent(ch, new TrieNode());
            current = current.children.get(ch);
        }
        current.isEndOfWord = true;
    }

    public boolean containsNegativeString(String goodsName) {
        TrieNode current = root;
        for (char ch : goodsName.toCharArray()) {
            if (current.isEndOfWord) {
                // 找到匹配的负向字符串
                return true;
            }
            if (!current.children.containsKey(ch)) {
                // 重置到根节点
                current = root;
            } else {
                current = current.children.get(ch);
            }
        }
        // 检查最后一个字符
        return current.isEndOfWord;
    }
}