# Scala Slick Driver for Machbase

It includes Scala Slick Driver (MachbaseProfile.scala) and its examples listed below;
* MachbaseSlickDriverTest.scala
* MachbaseSlickTypeTest.scala

You must check `lib/machbase.jar` is outdated or not. 
Unfortunately, it does not be controlled with any version information so update date will be helpful.
If you are not sure about this problem, please replace `lib/machbase.jar` file with `$MACHBASE_HOME/lib/machbase.jar`.

## Preparation

1. Install JDK 1.8 or above. Either Oracle Java or OpenJDK is okay.
1. Install sbt (linux) or Intellij IDEA (Windows).

## Compile & Run (Linux)
```bash
sbt compile
sbt run
```
After `sbt run`, please select one of two examples. (DriverTest is recommended)

## Compile & Run (Windows)
Select repository as project folder, and build them.
Press `F9` or execute 'Run > Run' menu.

After that, please select one of two examples. (DriverTest is recommended)
