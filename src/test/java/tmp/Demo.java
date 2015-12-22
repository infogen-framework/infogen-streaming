package tmp;

import java.io.IOException;

public class Demo {
	// public static void main(String[] args) {
	// Pattern pattern = Pattern.compile("^(a+)*$");
	// Matcher matcher = pattern.matcher("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaax");
	// boolean b= matcher.matches();
	// //当条件满足时，将返回true，否则返回false
	// System.out.println(b);
	// }
	@SuppressWarnings("restriction")
	public static void main(String[] args) throws IOException {
		String encode = new sun.misc.BASE64Encoder().encode("sdas".getBytes());
		System.out.println(encode);
		System.out.println( new String(new sun.misc.BASE64Decoder().decodeBuffer(encode)));
	}
}
