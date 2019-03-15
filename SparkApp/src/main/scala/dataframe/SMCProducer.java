package restclient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.RequestEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;


@SpringBootApplication
public class SMCProducer {
	public static void main(String[] args) throws IOException {
		String filename=args[0];//"D:\\datafiles\\input\\data_01_smc_source.xml";
		//filename is filepath string
		File file=new File(filename);
		
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line;
		StringBuilder sb = new StringBuilder();

		while((line=br.readLine())!= null){
			sb.append(line.trim());
		}
		
		BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("smcadmin", "AzxdM#m24"));
		HttpClient httpClient = HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build();
		ClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory(httpClient);

		RestTemplate restTemplateget = new RestTemplate(requestFactory);
		String postUrl = "http://uscdc01tlmap005:8082/topics/%2Fstreams%2Fsmcstream%3Asmcxmltopicpoc2";
		JSONObject obj=new JSONObject();
		obj.put(file.getName(), sb.toString());
		JSONObject fname = new JSONObject();
		JSONObject value = new JSONObject();
		JSONObject records = new JSONObject();
		fname.put("key", sb.toString());
		value.put("value", fname);
		JSONArray ja = new JSONArray();
		ja.put(value);
		records.put("records", ja);
		System.out.println(records);
		try{
			System.out.println("---------------END of strart producer---------------");
			HttpHeaders headers = new HttpHeaders();
			headers.set("Content-Type", "application/vnd.kafka.json.v1+json");
			System.out.println("------------------------------");			
			HttpEntity<String> entity = new HttpEntity<String>(records.toString(),headers);
			HttpEntity<String> response = restTemplateget.exchange(
					new URL(postUrl).toURI(), 
			        HttpMethod.POST, 
			        entity, 
			        String.class);		
			
			String testV=new JSONObject(response.getBody()).toString();

			System.out.println(testV);
			
		}catch(Exception e){
			e.printStackTrace();
		}	
        
	}
}
