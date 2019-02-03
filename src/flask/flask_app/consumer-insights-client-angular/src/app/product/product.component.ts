import { Component, OnInit } from '@angular/core';
import {ActivatedRoute} from "@angular/router";
import {Product} from "../../models/product.model.client";
import {ProductService} from "../../services/product.service.client";
import {Review} from "../../models/review.model.client";
import {ReviewService} from "../../services/review.service.client";

@Component({
  selector: 'app-product',
  templateUrl: './product.component.html',
  styleUrls: ['./product.component.css']
})
export class ProductComponent implements OnInit {

  constructor(private activatedRoute: ActivatedRoute, private productService: ProductService,
              private reviewService: ReviewService) {}

  product: Product;
  reviews: Review[];
  dummy = [];
  product_id = "";
  count = 0;
  ngOnInit() {
    this.activatedRoute.paramMap.subscribe(params => {
      this.productService.findProductById(params.get('productId')).then(
        response => {
          this.product = response;
          console.log(this.product);
          this.reviewService.findReviewByProduct(this.product["product_id"]).then(
            res => {
              this.count = res['hits']['total'];
              this.dummy = res['hits']['hits'];
              this.reviews = this.dummy.map(function (x) {return x["_source"]});
              console.log(this.reviews);
            }
          )
        });
    });
  }

  search = (feature) => {
    this.reviewService.findReviewBySearch(this.product.product_id, feature)
      .then(response => {
        this.count = response['hits']['total'];
        this.dummy = response['hits']['hits'];
        this.reviews = this.dummy.map(function (x) {return x["_source"]});
      });
    console.log(this.reviews);
  };

}
