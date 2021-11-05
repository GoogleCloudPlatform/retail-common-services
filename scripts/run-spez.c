/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <unistd.h>

#define MAX_ARGS 128

int main(int argc, char **argv) {
  char* args[MAX_ARGS];
  int i = 0;
  args[i++] = "java";
  args[i++] = "@/app/jvm-arguments";
  for (int j = 1; j < argc; j++) {
    args[i++] = argv[j];
  }
  args[i++] = "-jar";
  args[i++] = "Main.jar";
  args[i] = NULL;
  execvp("java", args);
  return 0;
}
