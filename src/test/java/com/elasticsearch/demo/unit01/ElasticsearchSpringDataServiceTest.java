package com.elasticsearch.demo.unit01;

import com.elasticsearch.demo.ElasticsearchApplication;
import com.elasticsearch.demo.vo.Employee;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liuming on 2017/11/12.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes= ElasticsearchApplication.class)
public class ElasticsearchSpringDataServiceTest {

    private final Logger logger = LoggerFactory.getLogger(getClass()) ;

    final String index = "megacorp_spring" ;
    final String type = "employee_spring" ;

    @Autowired
    private ElasticsearchSpringDataService elasticsearchSpringDataService ;

    @Test
    public void add() throws Exception {
        Employee employee = new Employee() ;
        employee.setId(4L);
        employee.setFirst_ame("John");
        employee.setLast_name("Smith");
        employee.setAge(25);
        employee.setAbout("I love to go rock climbing");
        List<String> interests = new ArrayList<>() ;
        interests.add("sports") ;
        interests.add("music") ;
        employee.setInterests(interests);

        /*employee.setId(2L);
        employee.setFirst_ame("Jane");
        employee.setLast_name("Smith");
        employee.setAge(32);
        employee.setAbout("I like to collect rock albums");
        List<String> interests = new ArrayList<>() ;
        interests.add("music") ;
        employee.setInterests(interests);*/

        /*employee.setId(3L);
        employee.setFirst_ame("Douglas");
        employee.setLast_name("Fir");
        employee.setAge(35);
        employee.setAbout("I like to build cabinets");
        List<String> interests = new ArrayList<>() ;
        interests.add("forestry") ;
        employee.setInterests(interests);*/

        elasticsearchSpringDataService.add(employee);
    }

    @Test
    public void getById()throws Exception{
        Long id = 1L ;
        Employee employee = elasticsearchSpringDataService.getById(id.toString() , Employee.class) ;
        ObjectMapper objectMapper = new ObjectMapper() ;
        String json = objectMapper.writeValueAsString(employee) ;
        logger.info("根据id:{}查找文档,结果:{}", id, json);
    }

    @Test
    public void getAll()throws Exception{
        List<Employee> employeeList = elasticsearchSpringDataService.getAll(Employee.class) ;
        out(employeeList) ;
    }

    private void out(List<Employee> employeeList)throws Exception{
        if(employeeList != null){
            logger.info("employeeList size : {} " , employeeList.size());
            ObjectMapper objectMapper = new ObjectMapper() ;
            for(Employee employee : employeeList){
                String json = objectMapper.writeValueAsString(employee) ;
                logger.info("查找文档,结果:{}", json);
            }
        }
    }

    @Test
    public void getByQueryString()throws Exception{
        List<Employee> employeeList = elasticsearchSpringDataService.getByQueryString("last_name:Smith", Employee.class) ;
        out(employeeList) ;
    }

    @Test
    public void getByQueryMatchDsl()throws Exception{
        List<Employee> employeeList = elasticsearchSpringDataService.getByQueryMatchDsl("last_name","Smith", Employee.class) ;
        out(employeeList) ;
        employeeList = elasticsearchSpringDataService.getByQueryMatchDsl("about","rock climbing", Employee.class) ;
        out(employeeList) ;
    }

    @Test
    public void getByMatchAndFilter()throws Exception{
        List<Employee> employeeList = elasticsearchSpringDataService.getByMatchAndFilter("last_name","Smith", "age", 30, Employee.class) ;
        out(employeeList) ;
    }

    @Test
    public void getByQueryMatchPhraseDsl() throws Exception {
        List<Employee> employeeList = elasticsearchSpringDataService.getByQueryMatchPhraseDsl("about","rock climbing", Employee.class) ;
        out(employeeList) ;
    }

    @Test
    public void getByQueryMatchPhraseAndHighlightDsl() throws Exception {
        List<Employee> employeeList = elasticsearchSpringDataService.getByQueryMatchPhraseAndHighlightDsl("about","rock climbing", Employee.class) ;
        out(employeeList) ;
    }

    @Test
    public void group() throws Exception {
        elasticsearchSpringDataService.group(index, type,"interests") ;
    }

    @Test
    public void groupAndQuery(){
        elasticsearchSpringDataService.groupAndQuery(index, type, "interests", "last_name", "smith") ;
    }

    @Test
    public void groupAndAvg(){
        elasticsearchSpringDataService.groupAndAvg(index, type, "interests", "age") ;
    }


}