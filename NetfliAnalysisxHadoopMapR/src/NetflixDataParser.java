

public class NetflixDataParser {
	
	private String year;
	private String movieName;
	private String rating;
	
	public void parse(String value){
		String[] array = value.split(",");
		year = array[4];
		movieName = array[0];
		rating = array[5];
		if(year.length()!=4){
			for(int i=5;i<array.length;i++){
				if(array[i].length()==4){
					year = array[i];
					rating = array[i+1];
					break;
				}
			}
		}
		
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}
	
	public String getMovieName() {
		return movieName;
	}

	public void setMovieName(String movieName) {
		this.movieName = movieName;
	}

	public String getRating() {
		return rating;
	}

	public void setRating(String rating) {
		this.rating = rating;
	}

	boolean isHeader(){
		if(year.equals("release year")){
			return true;
		}
		return false;
	}
	
	boolean isYearValid(){
		if(year.equals("") || year==null){
			return false;
		}
		return true;
	}
	
	boolean isRatingValid(){
		if(rating.equals("NA") || rating==null){
			return false;
		}
		return true;
	}
	

}


