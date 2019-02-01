import {Injectable} from "@angular/core";

@Injectable()
export class ProductService{

  url = "http://localhost:5000/api/product";

  findAllProducts = () =>
    fetch(this.url).then(res => res.json());

  findProductBySearch = keyword =>
    fetch(this.url+"/search/"+keyword).then(res => res.json());

  findProductById = productId =>
    fetch(this.url+"/"+productId).then(res => res.json());

}
