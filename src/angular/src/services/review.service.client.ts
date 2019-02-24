import {Injectable} from "@angular/core";

@Injectable()
export class ReviewService{

  url = "http://ec2-54-82-141-80.compute-1.amazonaws.com:5000/api/product/";
  // url = "http://localhost:5000/api/product/";

  findAllReviews = product_id =>
    fetch(this.url+product_id+"/review").then(res => res.json());

  findReviewBySearch = (product_id, keyword) =>
    fetch(this.url+product_id+"/review/search/"+keyword).then(res => res.json());

  findReviewByProduct = productId =>
    fetch(this.url+productId+"/review").then(res => res.json());

}
