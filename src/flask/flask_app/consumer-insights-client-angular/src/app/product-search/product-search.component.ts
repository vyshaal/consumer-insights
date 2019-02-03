import { Component, OnInit } from '@angular/core';
import {Product} from "../../models/product.model.client";
import {ProductService} from "../../services/product.service.client";
import {Router} from "@angular/router";

@Component({
  selector: 'app-product-search',
  templateUrl: './product-search.component.html',
  styleUrls: ['./product-search.component.css']
})
export class ProductSearchComponent implements OnInit {

  constructor(private productService: ProductService, private router: Router) {}

  feature = "";
  products: Product[];
  dummy = [];
  count = 0;
  search = (feature) => {
    // alert("Searching for: " + feature);
    this.productService.findProductBySearch(feature)
      .then(response => {
        this.count = response['hits']['total'];
        this.dummy = response['hits']['hits'];
        this.products = this.dummy.map(function (x) {return x["_source"]});
      });
    console.log(this.products);
  };


  ngOnInit() {
  }

}
