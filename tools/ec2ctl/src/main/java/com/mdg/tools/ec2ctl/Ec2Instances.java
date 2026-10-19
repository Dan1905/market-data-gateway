package com.mdg.tools.ec2ctl;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;

/**
 * The four EC2 calls ec2ctl makes, through the AWS SDK for Java v2.
 *
 * <p>Credentials and region come from the SDK's default chain: environment variables, then
 * {@code ~/.aws/credentials} and {@code ~/.aws/config} (what {@code aws configure} writes), then
 * an SSO session. Nothing is read from this repository.
 */
class Ec2Instances {

    private final Ec2Client ec2;

    Ec2Instances(Ec2Client ec2) {
        this.ec2 = ec2;
    }

    Instance describe(String instanceId) {
        return ec2.describeInstances(DescribeInstancesRequest.builder().instanceIds(instanceId).build())
                .reservations().stream()
                .map(Reservation::instances)
                .flatMap(java.util.List::stream)
                .findFirst()
                .map(i -> new Instance(i.instanceId(), i.state().nameAsString(), i.publicIpAddress()))
                .orElseThrow(() -> new IllegalStateException("No instance " + instanceId + " in this account and region"));
    }

    /** Starts the instance and blocks until EC2 reports it running (typically under a minute). */
    Instance startAndWait(String instanceId) {
        ec2.startInstances(StartInstancesRequest.builder().instanceIds(instanceId).build());
        ec2.waiter().waitUntilInstanceRunning(DescribeInstancesRequest.builder().instanceIds(instanceId).build());
        return describe(instanceId);
    }

    /** Stops the instance and blocks until EC2 reports it stopped. */
    Instance stopAndWait(String instanceId) {
        ec2.stopInstances(StopInstancesRequest.builder().instanceIds(instanceId).build());
        ec2.waiter().waitUntilInstanceStopped(DescribeInstancesRequest.builder().instanceIds(instanceId).build());
        return describe(instanceId);
    }
}
