package com.chinagoods.bigdata.functions.risk.negative;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xiaowei.song
 * @version v1.0.0
 * @description TODO
 * @date 2023/9/6 10:06
 */
class TrieNode {
    Map<Character, TrieNode> children;
    boolean isEndOfWord;

    public TrieNode() {
        children = new HashMap<>();
        isEndOfWord = false;
    }
}