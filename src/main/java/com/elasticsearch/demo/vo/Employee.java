package com.elasticsearch.demo.vo;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.List;

/**
 * Created by liuming on 2017/11/12.
 */
@Document(indexName = "megacorp_spring", type = "employee_spring")
public class Employee {

    @Id
    private Long id ;

    private String first_ame;
    private String last_name;
    private int age;
    private String about;
    private List<String> interests;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getFirst_ame() {
        return first_ame;
    }

    public void setFirst_ame(String first_ame) {
        this.first_ame = first_ame;
    }

    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getAbout() {
        return about;
    }

    public void setAbout(String about) {
        this.about = about;
    }

    public List<String> getInterests() {
        return interests;
    }

    public void setInterests(List<String> interests) {
        this.interests = interests;
    }
}
