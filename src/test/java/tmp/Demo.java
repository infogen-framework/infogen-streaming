package tmp;

public class Demo {
	// public static void main(String[] args) {
	// Pattern pattern = Pattern.compile("^(a+)*$");
	// Matcher matcher = pattern.matcher("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaax");
	// boolean b= matcher.matches();
	// //当条件满足时，将返回true，否则返回false
	// System.out.println(b);
	// }
	public static void main(String[] args) {
		String key = "hdfs://nameservice1/user/xuyufei";
		System.out.println(key.substring(0, key.lastIndexOf("/")));
	}
}
