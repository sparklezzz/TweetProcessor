package util;

import java.util.ArrayList;
import java.util.StringTokenizer;



public class TweetParser {

	private static final int _VALID_MAX_ORIGINAL_CHAR_NUM = 35;
	
    public long countWord(String body) {
  	  StringTokenizer tokenizer = new StringTokenizer(body);	  
  	  return tokenizer.countTokens();
    }  
    
    public long countChar(String body) {
  	  return body.length();
    }  
    
    public long countHashtag(String body) {
  	  StringTokenizer tokenizer = new StringTokenizer(body);	  
  	  String word;
  	  long count = 0;
  	  while (tokenizer.hasMoreTokens()) {
  		  word = tokenizer.nextToken();    		  
  		  if (word.charAt(0) == '#') {
  			  if (word.length() == 1) {
  				  continue;
  			  }
  			  count ++;
  		  }
  	  }
  	  return count;
    }
    
    public long countURL(String body) {
  	  StringTokenizer tokenizer = new StringTokenizer(body);	  
  	  String word;
  	  long count = 0;
  	  while (tokenizer.hasMoreTokens()) {
  		  word = tokenizer.nextToken();
  		  
  		  int pos1;
  		  if (word.startsWith("http://")) {
  			  count ++;
  		  }
  		  else if ((pos1 = word.indexOf(".com")) > 0) {	// suffix appear in the non-first pos
  			  count ++;
  		  }
  		  else if ((pos1 = word.indexOf(".gov")) > 0){	    			
  			  count ++;
  		  }
  		  else if ((pos1 = word.indexOf(".org")) > 0){	    			
  			  count ++;
  		  }
  		  else if ((pos1 = word.indexOf(".net")) > 0){	    			
  			  count ++;
  		  }
  		  else if ((pos1 = word.indexOf(".edu")) > 0){	    			
  			  count ++;
  		  }
  		  else if ((pos1 = word.indexOf(".cn")) > 0){	    			
  			  count ++;
  		  }    		  
  	  }
  	  return count;
    }    
    
    public boolean isOriginalTweet(String body) {
    	StringTokenizer tokenizer = new StringTokenizer(body);	  
      	String word;
      	
      	while (tokenizer.hasMoreTokens()) {
		  word = tokenizer.nextToken();    		  
		  if (word.equals("RT:") || word.equals("RT")) {
			return false;  
		  }
      	}
      	return true;
    }
    
    public boolean isRetweet(String body) {
  	  return !isOriginalTweet(body);
    }
    
    public long countMention(String body) {
  	  StringTokenizer tokenizer = new StringTokenizer(body);	  
  	  String word;
  	  long count = 0;
  	  while (tokenizer.hasMoreTokens()) {
  		  word = tokenizer.nextToken();
  		  if (word.charAt(0) == '@') {
  			  count ++;
  		  }
  		  else if (word.equals("RT") || word.equals("RT:")) {
  			  break;	//we ignore the @ after RT 
  		  }
  	  }
  	  return count;
    }
    
    /*
     *  return: 1. Most Original UserName 
     *  2. Original Part 
     *  3. Original Part Restricted
     */
    public String[] getOriginalPartFromRetweet(String body) {
    	String [] res = new String[3];
    	StringTokenizer tokenizer = new StringTokenizer(body);	  
      	String word;
      	
      	StringBuffer _buffer = new StringBuffer();
      	String lastUser = null;
      	
      	while (tokenizer.hasMoreTokens()) {
      		word = tokenizer.nextToken();    		      	            		  
			if (word.equals("RT") || word.equals("RT:")) {
				_buffer.delete(0, _buffer.length());	// rewrite original part
				if (tokenizer.hasMoreTokens()) {	
					word = tokenizer.nextToken();
					if (word.charAt(0) == '@') {
						lastUser = word.substring(1);	// override current user
						if (lastUser != null &&			// normalize username
								lastUser.length() > 0 &&
								lastUser.charAt(lastUser.length()-1) == ':') {
							lastUser = lastUser.substring(0, lastUser.length()-1);
						}
					}
					else if (word.equals("RT") || word.equals("RT:")) {
						continue;
					}
					else {
						_buffer.append(word + " ");		// regard as a normal word
					}
				}       
			}
			else {
				_buffer.append(word + " ");
			}
      	}
      	
      	String originalPart = _buffer.toString().trim();
      	if (lastUser == null || lastUser.isEmpty() || originalPart.length() < 3)
      		return null;
      	
      	res[0] = lastUser.toLowerCase();
      	res[1] = originalPart;
      	
      	StringBuffer _restrictBuffer = new StringBuffer();
      	String lowerOriginalPart = originalPart.toLowerCase();
      	for (int i=0; i<lowerOriginalPart.length(); i++) {
      		char c = lowerOriginalPart.charAt(i);
      		if (c >= 'a' && c <= 'z') {
      			_restrictBuffer.append(c);
      		}
      	}
      	String restrictPart = _restrictBuffer.toString();
      	int restrictLen = Math.min(restrictPart.length(), _VALID_MAX_ORIGINAL_CHAR_NUM);
      	res[2] = restrictPart.substring(0, restrictLen);
      	
      	return res;
    }
    
    public ArrayList<String> getMentionedUserList(String body) {
    	  ArrayList<String> res = new ArrayList<String>();
    	  StringTokenizer tokenizer = new StringTokenizer(body);	  
	   	  String word;
	   	  while (tokenizer.hasMoreTokens()) {
	   		  word = tokenizer.nextToken();    		      	            		  
	   		  if (word.equals("RT") || word.equals("RT:")) {	//ignore word after RT
	   			  break;
	   		  }
	   			 
			  if (word.charAt(0) == '@') {
				  String toUser = word.substring(1);
				  if (toUser != null &&			// normalize username
							toUser.length() > 0 &&
							toUser.charAt(toUser.length()-1) == ':') {
						toUser = toUser.substring(0, toUser.length()-1);
				  }
				  toUser = toUser.toLowerCase();
				  if (toUser != null || !toUser.isEmpty())
				  	  res.add(toUser);
				  
					  	// k:a, v:(a,b) , k:b v:(a,b)
			  }          			              		  
	   	  }
	   	  return res;
    }
    
    public String GetRetweetUser(String body) {
  	  StringTokenizer tokenizer = new StringTokenizer(body);	  
  	  String word;
  	  while (tokenizer.hasMoreTokens()) {
  		  word = tokenizer.nextToken();    		      	            		  
  		  if (word.equals("RT") || word.equals("RT:")) {
  			  if (tokenizer.hasMoreTokens()) {	// extract retweeted user after RT symbol
  				  word = tokenizer.nextToken();
  				  if (word.charAt(0) == '@') {
  					  String toUser = word.substring(1);
	  				  if (toUser != null &&			// normalize username
								toUser.length() > 0 &&
								toUser.charAt(toUser.length()-1) == ':') {
							toUser = toUser.substring(0, toUser.length()-1);
					  }
					  toUser = toUser.toLowerCase();
  					  return toUser;
  				  }
  			  }            			  
  		  }
  	  }
  	  return null;
    }
    
    public boolean ContainsUserName(String body, String userName) {
    	 StringTokenizer tokenizer = new StringTokenizer(body);	  
     	  String word;
     	  while (tokenizer.hasMoreTokens()) {
     		  word = tokenizer.nextToken();
     		 if (word.charAt(0) == '@') {
     			String toUser = word.substring(1).toLowerCase();
				  if (toUser != null &&			// normalize username
							toUser.length() > 0 &&
							toUser.charAt(toUser.length()-1) == ':') {
						toUser = toUser.substring(0, toUser.length()-1);
				  }
				  toUser = toUser.toLowerCase();
     			if (toUser.equals(userName)) {
     				return true;
     			}
     		 }
     	  }
     	  return false;
    }
}
