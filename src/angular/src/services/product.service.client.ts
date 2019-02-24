import {Injectable} from "@angular/core";

@Injectable()
export class ProductService{

  url = "http://ec2-54-82-141-80.compute-1.amazonaws.com:5000/api/product";
  // url = "http://localhost:5000/api/product";

  findAllProducts = () =>
    fetch(this.url).then(res => res.json());

  findProductBySearch = keyword =>
    fetch(this.url+"/search/"+keyword).then(res => res.json());

  findProductById = productId =>
    fetch(this.url+"/"+productId).then(res => res.json());

}
