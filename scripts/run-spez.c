#include <unistd.h>

#define MAX_ARGS 128

int main(int argc, char **argv) {
  char* args[MAX_ARGS];
  int i = 0;
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
