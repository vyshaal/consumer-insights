import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import {ProductService} from "../services/product.service.client";
import {ReviewService} from "../services/review.service.client";
import { ProductSearchComponent } from './product-search/product-search.component';
import {FormsModule} from "@angular/forms";
import { ProductComponent } from './product/product.component';
import {NgbModule} from "@ng-bootstrap/ng-bootstrap";


@NgModule({
  declarations: [
    AppComponent,
    ProductSearchComponent,
    ProductComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    FormsModule,
    NgbModule
  ],
  providers: [
    ProductService,
    ReviewService
  ],
  bootstrap: [AppComponent]
})
export class AppModule { }
