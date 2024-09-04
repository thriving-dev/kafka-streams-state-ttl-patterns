plugins {
    id("java")
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

dependencies {
    implementation("org.apache.avro:avro:1.12.0")
}

sourceSets {
    main {
        resources {
            srcDirs("src/main/avro")
        }
    }
}
