plugins {
    id("java")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

dependencies {
    annotationProcessor("org.projectlombok:lombok:1.18.34")
    implementation("io.confluent:kafka-schema-registry-client:7.7.0")
    implementation("io.confluent:kafka-streams-avro-serde:7.7.0")
    implementation("org.apache.kafka:kafka-clients")
    implementation("com.fasterxml.uuid:java-uuid-generator:5.1.0")

    implementation(project(":common-avros"))

    compileOnly("org.projectlombok:lombok:1.18.34")

    runtimeOnly("ch.qos.logback:logback-classic:1.5.7")
}

java {
    sourceCompatibility = JavaVersion.toVersion("21")
    targetCompatibility = JavaVersion.toVersion("21")
}

tasks {
    register<JavaExec>("produceBaggageTrackingEvents") {
        group = "application"
        description = "Produces BaggageTracking test data."
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("dev.thriving.poc.BaggageTrackingProducer")
    }
    register<JavaExec>("produceFlightEvents") {
        group = "application"
        description = "Produces Flight test data."
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("dev.thriving.poc.FlightProducer")
    }
    register<JavaExec>("produceUserFlightBookingEvents") {
        group = "application"
        description = "Produces UserFlightBooking test data."
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set("dev.thriving.poc.UserFlightBookingProducer")
    }
}
