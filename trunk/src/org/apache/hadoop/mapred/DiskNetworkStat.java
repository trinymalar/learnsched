package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class DiskNetworkStat {
	private static volatile boolean shouldRun = true;
	private static volatile double userCpu;
	private static volatile double ioTotalMb;
	private static Process diskStatProc;
	private static Process cpuStatProc;
	private static BufferedReader diskStatReader;
	private static BufferedReader cpuStatReader;
	private static final long MEGA = 1 << 20;
	private static double recv = 0;
	private static double tx = 0;

	static {
		try {
			initCollectors();
		} catch (IOException io) {
			cpuStatProc = null;
			diskStatProc = null;
			diskStatReader = null;
			cpuStatReader = null;
		}
	}

	private static void initCollectors() throws IOException {
		diskStatProc = new ProcessBuilder("iostat", "-dm", "5").start();
		cpuStatProc = new ProcessBuilder("iostat", "-c", "5").start();

		diskStatReader = new BufferedReader(new InputStreamReader(diskStatProc
				.getInputStream()));
		cpuStatReader = new BufferedReader(new InputStreamReader(cpuStatProc
				.getInputStream()));
		diskStatReader.readLine();
		cpuStatReader.readLine();

		DiskStatCollector diskth = new DiskStatCollector();
		CpuStatCollector cputh = new CpuStatCollector();
		diskth.setDaemon(true);
		cputh.setDaemon(true);
		diskth.start();
		cputh.start();
	}

	private static class DiskStatCollector extends Thread {
		public void run() {
			if (diskStatReader == null) {
				return;
			}
			while (shouldRun) {
				try {
					String line = diskStatReader.readLine().trim();
					if (line.startsWith("sda")) {
						String toks[] = line.split("\\s+");
						if (toks.length > 3) {
							double mbread = Double.parseDouble(toks[2]);
							double mbwrite = Double.parseDouble(toks[3]);
							// ioTotalMb = (mbread + mbwrite + ioTotalMb) * 0.5;
							ioTotalMb = mbread + mbwrite;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				if (diskStatReader != null && diskStatProc != null) {
					diskStatReader.close();
					diskStatProc.destroy();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	private static class CpuStatCollector extends Thread {
		public void run() {
			if (cpuStatReader == null) {
				return;
			}
			while (shouldRun) {
				try {
					String line = cpuStatReader.readLine().trim();
					if (!line.startsWith("avg") && line.length() > 0) {
						String toks[] = line.split("\\s+");
						if (toks.length > 1) {
							double ucpu = Double.parseDouble(toks[0]);
							// userCpu = (ucpu + userCpu) * 0.5;
							userCpu = ucpu;
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				if (cpuStatReader != null && cpuStatProc != null) {
					cpuStatReader.close();
					cpuStatProc.destroy();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static double getIO() {
		return ioTotalMb;
	}

	public static double getCpuUser() {
		return userCpu;
	}

	public static double getNetworkIO() {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new FileInputStream("/proc/net/dev")));
			String str = "";
			double in = 0;
			double out = 0;
			double del = 0;
			while ((str = br.readLine()) != null) {
				str = str.trim();
				if (str.startsWith("eth0:")) {
					String toks[] = str.split(":");
					String nums[] = toks[1].split("\\s+");
					in = Double.parseDouble(nums[0]);
					out = Double.parseDouble(nums[8]);
					del = in - recv + out - tx;
					recv = in;
					tx = out;
				}
			}
			br.close();
			return (del) / (5 * MEGA);
		} catch (Exception exc) {
			exc.printStackTrace();
			return 0;
		}
	}

	public static void main(String[] args) throws InterruptedException {
		while (true) {
			Thread.sleep(5000);
			System.out.println("IO:" + getIO());
			System.out.println("CPU User:" + getCpuUser());
			System.out.println(getNetworkIO());
		}
	}
}
