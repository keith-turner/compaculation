package compaculation.mgmt;

import static compaculation.mgmt.TieredCompactionManager.getSmallestFilesWithSumLessThan;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import compaculation.mgmt.SubmittedJob.Status;

public class TieredCopmactionManagerTest {
  
  private static final long M  = 1000000;
  
  @Test
  public void testFilterLargeFiles() {
    var testData = Map.of("f1",100L,"f2",99L,"f3",4L,"f4",2L);
    
    assertEquals( Map.of("f4",2L), getSmallestFilesWithSumLessThan(testData, 6));
    assertEquals( Map.of("f3", 4L,"f4",2L), getSmallestFilesWithSumLessThan(testData, 7));
    assertEquals( Map.of("f3", 4L,"f4",2L), getSmallestFilesWithSumLessThan(testData, 105));
    assertEquals( Map.of(), getSmallestFilesWithSumLessThan(testData, 1));
    assertEquals( Map.of("f2",99L,"f3", 4L,"f4",2L), getSmallestFilesWithSumLessThan(testData, 107));
    assertEquals( Map.of("f2",99L,"f3", 4L,"f4",2L), getSmallestFilesWithSumLessThan(testData, 205));
    assertEquals( Map.of("f1",100L,"f2",99L,"f3", 4L,"f4",2L), getSmallestFilesWithSumLessThan(testData, 206));
  }
  
  @Test
  public void testFindFiles() {
    
    var testData1 = Map.of("f1",100*M,"f2",75*M,"f3",33*M,"f4",33*M,"f5",33*M,"f6",1*M);
    testPlanning(testData1, 3, Set.of("f3","f4","f5","f6"), "large");
    
    var testData2 = Map.of("f0",100*M,"f1",100*M,"f2",75*M,"f3",33*M,"f4",33*M,"f5",33*M,"f6",1*M);
    testPlanning(testData2, 3, Set.of("f0","f1","f2","f3","f4","f5","f6"), "large");
    
    var testData3 = Map.of("f0",300*M,"f1",300*M,"f2",300*M,"f3",33*M,"f4",33*M,"f5",33*M,"f6",33*M,"f7",33*M);
    testPlanning(testData3, 3, Set.of("f3","f4","f5","f6","f7"), "large");
    
    var testData4 = Map.of("f0",300*M,"f1",300*M,"f2",300*M,"f8",165*M);
    testPlanning(testData4, 3, Set.of("f0","f1","f2","f8"), "huge");
    
    var testData5 = Map.of("B1",200*M,"B2",200*M,"M1",30*M,"M2",30*M,"M3",30*M,"M4",30*M,"S1",2*M,"S2",2*M,"S3",2*M,"S4",2*M);
    testPlanning(testData5, 3, Set.of("S1","S2","S3","S4"), "small");
  }
  
  @Test
  public void testRunning() {
    
    var srj = newRunningSubmittedJob("f1","f2","f3","f4");
    
    var testData1 = Map.of("f1",100*M,"f2",100*M,"f3",100*M,"f4",10*M,"f5",2*M,"f6",2*M,"f7",2*M,"f8", 1*M);
    testPlanning(testData1, 3, Set.of("f5","f6","f7","f8"), "small", List.of(srj));
    
    srj = newRunningSubmittedJob("f1","f2","f3","f5");
    testPlanning(testData1, 3, Set.of(), "small", List.of(srj));
    
    
    
    
    
    
  }
  
  private SubmittedJob newRunningSubmittedJob(String ... files) {
    var job = new Job(0, Arrays.asList(files), "");
    return new SubmittedJob(job, 4, Status.RUNNING);
  }
  
  private void testPlanning(Map<String, Long> files, double ratio, Set<String> expected, String executor) {
    testPlanning(files, ratio, expected, executor, List.of());
  }
  
  private void testPlanning(Map<String, Long> files, double ratio, Set<String> expected, String executor, List<SubmittedJob> submitted) {
    TieredCompactionManager tcm = new TieredCompactionManager(ratio);
    
    CompactionPlan plan = tcm.makePlan(files, submitted);
    
    assertEquals(List.of(), plan.cancellations);
    if(expected.isEmpty()) {
      assertEquals(0, plan.jobs.size());
    } else {
      assertEquals(1, plan.jobs.size());
      assertEquals(executor, plan.jobs.get(0).getExecutor());
      assertEquals(expected, plan.jobs.get(0).getFiles());
    }
  }
  
}
