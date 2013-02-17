/**
 * 
 */
package com.cloudack.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.lang.Character;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author pudi
 * 
 */

public class CommonFriendReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, new Text(commonFriendsInList(values)));
	}

	public String commonFriendsInList(Iterable<Text> values) {

		List<Text> frndList = new ArrayList<Text>();
		for (Text fVal : values) {
			frndList.add(new Text(fVal));
		}

		if (frndList.size() < 2) {
			return "";
		}

		Set<Character> ss1 = toSet(frndList.get(0).toString());
		ss1.retainAll(toSet(frndList.get(1).toString()));
		return ss1.toString();
	}

	public static Set<Character> toSet(String s) {
		Set<Character> ss = new HashSet(s.length());
		for (char c : s.toCharArray()) {
			if (c != ' ')
				ss.add(Character.valueOf(c));
		}
		return ss;
	}
}