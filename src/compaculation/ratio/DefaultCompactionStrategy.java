/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package compaculation.ratio;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

//code was copied from Accumulo and modified
public class DefaultCompactionStrategy {

  /**
   * Keeps track of the sum of the size of all files within a window. The files are sorted from
   * largest to smallest. Supports efficiently creating sub windows, sliding the window, and
   * shrinking the window.
   */
  public static class SizeWindow {

    List<CompactionFile> files;
    long sum = 0;

    int first;
    int last;

    SizeWindow() {}

    public SizeWindow(Map<String,Long> allFiles) {
      files = new ArrayList<>();
      for (Entry<String,Long> entry : allFiles.entrySet()) {
        files.add(new CompactionFile(entry.getKey(), entry.getValue()));
      }

      Collections.sort(files, Comparator.comparingLong(CompactionFile::getSize)
          .thenComparing(CompactionFile::getFile).reversed());

      for (CompactionFile file : files) {
        sum += file.size;
      }

      first = 0;
      last = files.size();
    }

    public void pop() {
      if (first >= last)
        throw new IllegalStateException("Can not pop");

      sum -= files.get(first).size;
      first++;
    }

    public long topSize() {
      return files.get(first).size;
    }

    public long bottomSize() {
      return files.get(last - 1).size;
    }

    public boolean slideTop() {
      if (first == 0)
        return false;

      first--;

      sum += files.get(first).size;

      return true;
    }

    public boolean slideBottom() {
      if (first >= last)
        return false;

      last--;

      sum -= files.get(last).size;

      return true;

    }

    public boolean slideUp() {
      if (first == 0)
        return false;

      first--;
      last--;

      sum += files.get(first).size;
      sum -= files.get(last).size;

      return true;
    }

    public SizeWindow tail(int windowSize) {
      // Preconditions.checkArgument(windowSize > 0);

      SizeWindow sub = new SizeWindow();

      sub.files = files;
      sub.first = Math.max(last - windowSize, first);
      sub.last = last;
      sub.sum = 0;

      for (int i = sub.first; i < sub.last; i++) {
        sub.sum += files.get(i).size;
      }

      return sub;
    }

    public long sum() {
      return sum;
    }

    public int size() {
      return (last - first);
    }

    public Set<String> getFiles() {
      Set<String> windowFiles = new HashSet<>(size());
      for (int i = first; i < last; i++) {
        windowFiles.add(files.get(i).file);
      }
      return windowFiles;
    }

    public SizeWindow clone() {
      SizeWindow clone = new SizeWindow();

      clone.files = files;
      clone.first = first;
      clone.last = last;
      clone.sum = sum;

      return clone;
    }

    @Override
    public String toString() {
      return "size:" + size() + " sum:" + sum() + " first:" + first + " last:" + last + " topSize:"
          + topSize();
    }
  }

  private static class CompactionFile {
    public String file;
    public long size;

    public CompactionFile(String file, long size) {
      super();
      this.file = file;
      this.size = size;
    }

    long getSize() {
      return size;
    }

    String getFile() {
      return file;
    }
  }

  public static Set<String> findMapFilesToCompact2(Map<String,Long> candidates, double ratio,
      int maxFilesToCompact, int maxFilesPerTablet) {
    SizeWindow all = new SizeWindow(candidates);

    if (candidates.size() <= 1)
      return null;

    SizeWindow best = null;

    SizeWindow window = all.tail(2);

    while (true) {
      if (window.topSize() * ratio <= window.sum()) {
        if (best == null || best.size() < window.size()) {
          best = window.clone();
          if (best.size() == maxFilesToCompact)
            break;
        }
      }

      if (!window.slideTop()) {
        break;
      }

      if (window.size() > maxFilesToCompact) {
        window.slideBottom();
      }

      while (window.size() > 1 && window.bottomSize() * 10 < window.topSize()) {
        window.slideBottom();
      }

    }

    if (best != null)
      return best.getFiles();

    return null;
  }

  public static Set<String> findMapFilesToCompact(Map<String,Long> candidates, double ratio,
      int maxFilesToCompact, int maxFilesPerTablet) {

    if (candidates.size() <= 1)
      return null;

    int minFilesToCompact = 0;
    if (candidates.size() > maxFilesPerTablet)
      minFilesToCompact = candidates.size() - maxFilesPerTablet + 1;

    minFilesToCompact = Math.min(minFilesToCompact, maxFilesToCompact);

    SizeWindow all = new SizeWindow(candidates);

    Set<String> files = null;

    // Within a window of size maxFilesToCompact containing the smallest files check to see if any
    // files meet the compaction ratio criteria.
    SizeWindow window = all.tail(maxFilesToCompact);
    while (window.size() > 1 && files == null) {

      if (window.topSize() * ratio <= window.sum()) {
        files = window.getFiles();
      }

      window.pop();
    }

    // Previous search was fruitless. If there are more files than maxFilesToCompact, then try
    // sliding the window up looking for files that meet the criteria.
    if (files == null || files.size() < minFilesToCompact) {
      window = all.tail(maxFilesToCompact);

      files = null;

      // When moving the window up there is no need to pop/shrink the window. All possible sets are
      // covered without doing this. Proof is left as an exercise for the reader. This is predicated
      // on the first search shrinking the initial window.
      while (window.slideUp() && files == null) {
        if (window.topSize() * ratio <= window.sum()) {
          files = window.getFiles();
        }
      }
    }

    // Ensure the minimum number of files are compacted.
    if ((files != null && files.size() < minFilesToCompact)
        || (files == null && minFilesToCompact > 0)) {
      // get the smallest files of size minFilesToCompact
      files = all.tail(minFilesToCompact).getFiles();
    }

    return files;
  }
}
