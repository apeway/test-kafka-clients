package org.liws.xt;

import org.junit.Test;

public class ByteMove {

	@Test
	public void leftMove() {
		System.out.println(Integer.toBinaryString(-1));
		
//		int num = 0x10000000;
//		System.out.println("哈哈哈原数据：" + num + "，\t二进制表示：" + Integer.toBinaryString(num));
//		for (int i = 1; i < 8; i++) {
//			num = num << 1;
//			System.out.println("第" + i + "次左移后：" + num + "，\t二进制表示：" + Integer.toBinaryString(num));
//		}
	}

	@Test
	public void rightMove() {
		int num = 0x80000000;
		System.out.println("哈哈哈原数据：" + num + "，\t二进制表示：" + Integer.toBinaryString(num));
		for (int i = 1; i < 8; i++) {
			num = num >> 1;
			System.out.println("第" + i + "次左移后：" + num + "，\t二进制表示：" + Integer.toBinaryString(num));
		}
	}
}