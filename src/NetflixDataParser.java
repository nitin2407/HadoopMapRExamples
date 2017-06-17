

public class NetflixDataParser {
	
	private String year;
	
	public void parse(String value){
		String[] array = value.split(",");
		year = array[4];
		if(year.length()!=4){
			for(int i=5;i<array.length;i++){
				if(array[i].length()==4){
					year = array[i];
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

}


