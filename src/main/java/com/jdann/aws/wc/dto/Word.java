package com.jdann.aws.wc.dto;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Comparator;
import java.util.Objects;

@DynamoDBTable(tableName = Word.TABLE_NAME)
public class Word {

    public static final String TABLE_NAME = "word_totals";
    public static final String ADDRESS = "address";
    public static final String WORD = "word";
    public static final String TOTAL = "total";

    @JsonIgnore
    private String address;
    private String word;
    private Long total;

    public Word(String address, String word) {
        this.address = address;
        this.word = word;
        this.total = 0L;
    }

    public Word() {}

    @DynamoDBHashKey
    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @DynamoDBRangeKey
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    @DynamoDBAttribute
    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public void inc() {
        this.total++;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Word)) return false;
        Word word = (Word) o;
        return Objects.equals(this.word, word.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word);
    }

    public static Comparator<Word> byTotal = (Word w1, Word w2) -> w2.getTotal().compareTo(w1.getTotal());

    @Override
    public String toString() {
        return "Word{" +
                "address='" + address + '\'' +
                ", word='" + word + '\'' +
                ", total=" + total +
                '}';
    }
}
