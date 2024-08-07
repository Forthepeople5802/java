
import com.google.gson.*;


import java.util.*;
import org.apache.commons.lang.StringUtils;

public class TopogySort {
    static class JobVo {
        String job_id;
        String job_name;
        String job_in_conditions;
        String job_out_conditions;

        JobVo(String job_id, String job_name, String job_in_conditions, String job_out_conditions) {
            this.job_id = job_id;
            this.job_name = job_name;
            this.job_in_conditions = job_in_conditions;
            this.job_out_conditions = job_out_conditions;
        }

        @Override
        public String toString() {
            return job_name + " (" + job_id + ")";
        }
    }

    public static JsonArray getOutConditionNames(JobVo data) {
        JsonParser parser = new JsonParser();
        JsonArray conditions = new JsonArray();
        String condition = data.job_out_conditions;

        if (StringUtils.isNotEmpty(condition)) {
            JsonElement jobElement = parser.parse(condition);
            if (jobElement.getAsJsonObject().has("eventsToAdd")) {
                JsonObject eventsObj = jobElement.getAsJsonObject().getAsJsonObject("eventsToAdd");

                if (eventsObj.has("Events")) {
                    conditions = eventsObj.getAsJsonArray("Events");
                }
            }
        }
        return conditions;
    }

    public static JsonArray getInConditionNames(JobVo data) {
        JsonParser parser = new JsonParser();
        JsonArray conditions = new JsonArray();
        String condition = data.job_in_conditions;

        if (StringUtils.isNotEmpty(condition)) {
            JsonElement jobElement = parser.parse(condition);
            if (jobElement.getAsJsonObject().has("eventsToWaitFor")) {
                JsonObject eventsObj = jobElement.getAsJsonObject().getAsJsonObject("eventsToWaitFor");

                if (eventsObj.has("Events")) {
                    conditions = eventsObj.getAsJsonArray("Events");
                }
            }
        }
        return conditions;
    }

    public static void main(String[] args) {
        List<JobVo> jobs = Arrays.asList(
            new JobVo("20240730000075", "FLOW_TEST_JOB_09", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_08-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_09-OK\"}]}}"),
            new JobVo("20240730000074", "FLOW_TEST_JOB_11", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB10-OK\"},{\"Event\":\"FLOW_TEST_JOB_09-OK\"},{\"Event\":\"FLOW_TEST_JOB_01-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_11-OK\"}]},\"eventsToDelete\":{\"Type\":\"DeleteEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_11_R-OK\"}]}}"),
            new JobVo("20240730000073", "FLOW_TEST_JOB_10", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_08-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_10-OK\"}]}}"),
            new JobVo("20240730000072", "FLOW_TEST_JOB_01", null, "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_01-OK\"}]}}"),
            new JobVo("20240730000072", "FLOW_TEST_JOB_01", null, "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_01-OK\"}]}}"),
            new JobVo("20240730000071", "FLOW_TEST_JOB_02", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_01-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_02-OK\"}]}}"),
            new JobVo("20240730000070", "FLOW_TEST_JOB_04", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_03-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_04-OK\"}]}}"),
            new JobVo("20240730000069", "FLOW_TEST_JOB_05", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_02-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_05-OK\"}]}}"),
            new JobVo("20240730000068", "FLOW_TEST_JOB_03", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_02-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_03-OK\"}]}}"),
            new JobVo("20240730000067", "FLOW_TEST_JOB_06", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_04-OK\"},{\"Event\":\"FLOW_TEST_JOB_05-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_06-OK\"},{\"Event\":\"123123\"}]}}"),
            new JobVo("20240730000066", "FLOW_TEST_JOB_07", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_06-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_07-OK\"}]}}"),
            new JobVo("20240730000065", "FLOW_TEST_JOB_08", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_07-OK\"},{\"Event\":\"FLOW_TEST_JOB_11_R-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_08-OK\"}]}}"),
            new JobVo("20240730000999", "FLOW_TEST_JOB_999", null, null)
            
//            new JobVo("20240730000072", "FLOW_TEST_JOB_01", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_03-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_01-OK\"}]}}"),
//            new JobVo("20240730000071", "FLOW_TEST_JOB_02", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_01-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_02-OK\"}]}}"),
//            new JobVo("20240730000068", "FLOW_TEST_JOB_03", "{\"eventsToWaitFor\":{\"Type\":\"WaitForEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_02-OK\"}]}}", "{\"eventsToAdd\":{\"Type\":\"AddEvents\",\"Events\":[{\"Event\":\"FLOW_TEST_JOB_03-OK\"}]}}")
        );
        
        List<String> order = getOrder(jobs);
        if (order == null) {
            System.out.println("작업 순서를 구할 수 없습니다 (순환 종속성 존재).");
        } else {
            System.out.println("작업 순서: " + String.join(" -> ", order));
            jobs.sort(Comparator.comparingInt(job -> {
                JsonArray outConditions = getOutConditionNames(job);
                if (outConditions.size() == 0) {
                    return Integer.MAX_VALUE; // outCondition이 없는 작업은 마지막에 정렬
                }
                return order.indexOf(outConditions.get(0).getAsJsonObject().get("Event").getAsString());
            }));
            System.out.println("정렬된 작업:");
            for (JobVo job : jobs) {
                System.out.println(job);
            }
        }
    }

    private static List<String> getOrder(List<JobVo> jobs) {
        // 각 작업의 진입 차수를 저장하는 맵
        Map<String, Integer> inDegree = new HashMap<>();
        // 각 작업의 다음 작업들을 저장하는 맵
        Map<String, List<String>> graph = new HashMap<>();
        
        // 초기화
        for (JobVo job : jobs) {
            JsonArray outConditions = getOutConditionNames(job);
            for (JsonElement outCondition : outConditions) {
                String outEvent = outCondition.getAsJsonObject().get("Event").getAsString();
                inDegree.put(outEvent, 0);
                graph.put(outEvent, new ArrayList<>());
            }
            JsonArray inConditions = getInConditionNames(job);
            for (JsonElement inCondition : inConditions) {
                String inEvent = inCondition.getAsJsonObject().get("Event").getAsString();
                if (!graph.containsKey(inEvent)) {
                    graph.put(inEvent, new ArrayList<>());
                }
                if (!inDegree.containsKey(inEvent)) {
                    inDegree.put(inEvent, 0);
                }
            }
        }

        // 그래프 구성 및 진입 차수 계산
        for (JobVo job : jobs) {
            JsonArray inConditions = getInConditionNames(job);
            JsonArray outConditions = getOutConditionNames(job);
            for (JsonElement inCondition : inConditions) {
                String inEvent = inCondition.getAsJsonObject().get("Event").getAsString();
                for (JsonElement outCondition : outConditions) {
                    String outEvent = outCondition.getAsJsonObject().get("Event").getAsString();
                    /**
                     *	그래프에 노드 연결.. 선행에 연결된 후행을추가
                     *  FLOW_TEST_JOB_01-OK=[FLOW_TEST_JOB_11-OK, FLOW_TEST_JOB_02-OK]
                     * */
                    graph.get(inEvent).add(outEvent);
                    /**
                     * 차수 계산 연결된게 없으면 0, 1개면 1, 2개면 2
                     * FLOW_TEST_JOB_06-OK=2
                     * FLOW_TEST_JOB10-OK=0
                     * FLOW_TEST_JOB_10-OK=1
                     * FLOW_TEST_JOB_11-OK=3
                     * */
                    // 
                    inDegree.put(outEvent, inDegree.get(outEvent) + 1);
                }
            }
        }

        // 진입 차수가 0인 작업들을 큐에 추가
        Queue<String> queue = new LinkedList<>();
        for (String job : inDegree.keySet()) {
            if (inDegree.get(job) == 0) {
                queue.add(job);
            }
        }

        // 위상 정렬 수행
        List<String> result = new ArrayList<>();
        while (!queue.isEmpty()) {
        	/**
        	 * 큐에 있는 진인차수 0인 작업들을 꺼내서
        	 * 위상정렬 결과값에 추가한다. 
        	 * 그래프에 연결되어있는 다음 작업에 진입차수를 -1하고
        	 * 차수가 0이 되었다면 큐에 담는다.
        	 * 모든 차수가 0이 되고, 큐에 남는게 없을때까지 반복.. 
        	 * */
        	
            String current = queue.poll();
            result.add(current);

            for (String next : graph.get(current)) {
                inDegree.put(next, inDegree.get(next) - 1);
                if (inDegree.get(next) == 0) {
                    queue.add(next);
                }
            }
        }

        // 모든 작업을 처리했는지 확인
        if (result.size() != inDegree.size()) {
        	// 크기가 다르다면 순환 종속성이 존재 한다.
            return null; 
        }

        return result;
    }
}

