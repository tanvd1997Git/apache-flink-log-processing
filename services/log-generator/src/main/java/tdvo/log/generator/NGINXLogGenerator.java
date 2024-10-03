package tdvo.log.generator;

import com.jsoniter.output.JsonStream;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Random;

public class NGINXLogGenerator {

	@AllArgsConstructor
	private static class NGINXLog {
		String remoteAddress;
		String httpMethods;
		String URLPath;
		String userAgent;
		String referer;
		int statusCode;
		int bodyBytesSent;
		double responseTime;
		long createdTime;
	}

	private static final String[] IP_ADDRESSES = {
			"192.168.1.1", "10.0.0.2", "172.16.0.3", "203.0.113.4", "198.51.100.5"
	};
	private static final String[] HTTP_METHODS = {"GET", "POST", "PUT", "DELETE"};
	private static final String[] URL_PATHS = {
			"/index.html", "/login", "/register", "/profile", "/products", "/api/data"
	};
	private static final String[] USER_AGENTS = {
			"Mozilla/5.0", "Chrome/91.0", "Safari/537.36", "Opera/9.80"
	};
	private static final String[] REFERRERS = {
			"-", "http://example.com", "http://google.com", "http://yahoo.com"
	};
	private static final int[] STATUS_CODES = {200, 201, 202, 203, 204, 301, 400, 500};

	public static String generate() {
		Random random = new Random();
		NGINXLog nginxLog = new NGINXLog(
				IP_ADDRESSES[random.nextInt(IP_ADDRESSES.length)],
				HTTP_METHODS[random.nextInt(HTTP_METHODS.length)],
				URL_PATHS[random.nextInt(URL_PATHS.length)],
				USER_AGENTS[random.nextInt(USER_AGENTS.length)],
				REFERRERS[random.nextInt(REFERRERS.length)],
				STATUS_CODES[random.nextInt(STATUS_CODES.length)],
				random.nextInt(100_000),
				random.nextDouble() * 1.23456,
				Instant.now().toEpochMilli()
		);
		return JsonStream.serialize(nginxLog);
	}
}
