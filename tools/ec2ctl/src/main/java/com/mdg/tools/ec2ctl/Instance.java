package com.mdg.tools.ec2ctl;

/**
 * What ec2ctl needs to know about the instance.
 *
 * @param publicIp {@code null} while the instance is stopped: AWS releases an automatically
 *                 assigned public IP on stop, and hands out a new one on start
 */
record Instance(String id, String state, String publicIp) {

    boolean isRunning() {
        return "running".equals(state);
    }

    boolean isStopped() {
        return "stopped".equals(state);
    }
}
