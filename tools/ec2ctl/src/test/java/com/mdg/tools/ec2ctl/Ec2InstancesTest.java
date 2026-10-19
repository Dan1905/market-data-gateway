package com.mdg.tools.ec2ctl;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.InstanceStateName;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;
import software.amazon.awssdk.services.ec2.waiters.Ec2Waiter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** The SDK calls themselves, against a mocked {@link Ec2Client}: right requests, right order. */
class Ec2InstancesTest {

    private final Ec2Client client = mock(Ec2Client.class);
    private final Ec2Waiter waiter = mock(Ec2Waiter.class);
    private final Ec2Instances instances = new Ec2Instances(client);

    private static DescribeInstancesResponse described(String state, String ip) {
        return DescribeInstancesResponse.builder().reservations(Reservation.builder().instances(
                software.amazon.awssdk.services.ec2.model.Instance.builder()
                        .instanceId("i-1").publicIpAddress(ip)
                        .state(s -> s.name(InstanceStateName.fromValue(state))).build()).build()).build();
    }

    @Test
    @DisplayName("describe maps the SDK response to state and public IP")
    void describe() {
        when(client.describeInstances(any(DescribeInstancesRequest.class))).thenReturn(described("running", "13.61.174.17"));

        Instance instance = instances.describe("i-1");

        assertThat(instance).isEqualTo(new Instance("i-1", "running", "13.61.174.17"));
        assertThat(instance.isRunning()).isTrue();
    }

    @Test
    @DisplayName("an unknown instance id is a clear error, not an empty result")
    void unknownInstance() {
        when(client.describeInstances(any(DescribeInstancesRequest.class))).thenReturn(DescribeInstancesResponse.builder().build());

        assertThatThrownBy(() -> instances.describe("i-nope")).hasMessageContaining("No instance i-nope");
    }

    @Test
    @DisplayName("start sends StartInstances, then waits for 'running', then reads the new IP")
    void startWaitsForRunning() {
        when(client.waiter()).thenReturn(waiter);
        when(client.describeInstances(any(DescribeInstancesRequest.class))).thenReturn(described("running", "13.61.200.5"));

        Instance started = instances.startAndWait("i-1");

        var order = inOrder(client, waiter);
        order.verify(client).startInstances(StartInstancesRequest.builder().instanceIds("i-1").build());
        order.verify(waiter).waitUntilInstanceRunning(DescribeInstancesRequest.builder().instanceIds("i-1").build());
        assertThat(started.publicIp()).isEqualTo("13.61.200.5");
    }

    @Test
    @DisplayName("stop sends StopInstances, then waits for 'stopped'")
    void stopWaitsForStopped() {
        when(client.waiter()).thenReturn(waiter);
        when(client.describeInstances(any(DescribeInstancesRequest.class))).thenReturn(described("stopped", null));

        Instance stopped = instances.stopAndWait("i-1");

        var order = inOrder(client, waiter);
        order.verify(client).stopInstances(StopInstancesRequest.builder().instanceIds("i-1").build());
        order.verify(waiter).waitUntilInstanceStopped(DescribeInstancesRequest.builder().instanceIds("i-1").build());
        assertThat(stopped.isStopped()).isTrue();
        assertThat(stopped.publicIp()).isNull();
    }
}
