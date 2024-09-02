plugins {
    idea
}

version = "0.0.1"
group = "dev.thriving.poc"

allprojects {
    repositories {
        mavenCentral()
        maven("https://packages.confluent.io/maven/")
    }
}

