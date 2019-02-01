import {Injectable} from "@angular/core";

@Injectable()
export class ReviewService{

  url = "http://localhost:5000/api/product/";

  findAllReviews = product_id =>
    fetch(this.url+product_id+"/review").then(res => res.json());

  findReviewBySearch = keyword =>
    fetch(this.url+"/search/"+keyword).then(res => res.json());

  findReviewByProduct = productId =>
    fetch(this.url+"/"+productId+"/review").then(res => res.json());

}
