package com.galaxy.neptune;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.ksyun.kmr.hadoop.fs.ks3.Ks3FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class KSYunKS3Integration {

    public static void main(String[] args) throws IOException, URISyntaxException {
        //ks3私有云
        String accessKey = "AKLTf4R-MdVORt2vt6wCj-LQUw";
        String secretKey = "OCvFaiWKodwRc+yw3hjB0ErwewYj+G6gghSN2m029vJztbCc4djMOmuNzLr+tDGqiQ==";
        String endPoint = "http://ks3-cn-shanghai-2.cqpcloud.cn";
        String region = "local-region";
        String bucketName = "cqyxy50011401";
        //ks3公有云
        /*String accessKey = "AKLTZS7PtVTLREyQJe72fMZh";
        String secretKey = "OCAFSSUP56o7mDPnAT7Y8Z8wOsqQM3aayB8mvhOf";
        String endPoint = "http://ks3-cn-beijing.ksyuncs.com";
        String region = "local-region";
        String bucketName = "fengyeyuxiang";*/

        //minio
//        String accessKey = "AKIAIOSFODNN7EXAMPLE";
//        String secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
//        String endPoint = "http://10.128.23.219:9000";
//        String region = "local-region";
//        String bucketName = "goofys";
        Configuration conf = new Configuration();
        conf.set("fs.ks3.AccessKey", accessKey);
        conf.set("fs.ks3.AccessSecret", secretKey);
        conf.set("fs.s3a.endpoint", endPoint);
        conf.set("fs.ks3.impl", "com.ksyun.kmr.hadoop.fs.ks3.Ks3FileSystem");
//        conf.set(FS_KS3_IMPL_DISABLE_CACHE, disableCache);
        FileSystem ks3FileSystem = FileSystem.get(new URI("ks3://ks3-cn-shanghai-2.cqpcloud.cn"),conf);
        FileStatus[] fileStatuses = ks3FileSystem.listStatus(new Path("ks3://cqyxy000152/"));
        System.out.println(fileStatuses.length);
//        FSDataInputStream inputStream = ks3FileSystem.open(new Path("ks3://cqyxy000152/lltest"));
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
//        System.out.println(bufferedReader.readLine());
//        FsStatus status = ks3FileSystem.getStatus();
//        System.out.println(ks3FileSystem.open(new Path("")));
    }
}
